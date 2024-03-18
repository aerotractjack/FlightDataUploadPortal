import sys
import re
from PyQt6.QtWidgets import (QApplication, QWizard, QVBoxLayout, 
                    QLabel, QPushButton, QFileDialog, QWizardPage,
                    QListWidget, QMessageBox, QComboBox)
from PyQt6.QtGui import QFont
import json
import integration
import persistqueue
from persistqueue.serializers import json as pq_json
import os
import platform
from pathlib import Path, PureWindowsPath
from dotenv import load_dotenv
import pandas as pd
from filelock import FileLock

from aerologger import AeroLogger
dup_logger = AeroLogger(
    'SD Uploader',
    'SD_DUP/SD_DUP.log'
)

# setup upload queue
platform_name = platform.system()
is_linux = platform_name == "Linux"

# linux (hopefully) or windows
if is_linux:
    load_dotenv("/home/aerotract/NAS/main/software/db_env.sh")
    sq_path = Path(os.getenv("STORAGE_QUEUE_PATH"))
    lock_path = os.getenv("STORAGE_QUEUE_LOCK_PATH")
else:
    load_dotenv("Z:\\software\\db_env.sh")
    base = Path(os.path.expanduser("~"))
    sq_path = os.getenv("STORAGE_QUEUE_WINDOWS_PATH")
    sq_path = base / sq_path
    lock_path = (base / Path(os.getenv("STORAGE_QUEUE_LOCK_WINDOWS_PATH"))).as_posix()
sq_path = Path(sq_path)

if not sq_path.exists():
    sq_path.mkdir(parents=True, exist_ok=True)

uploadQ = persistqueue.Queue(sq_path, autosave=True, serializer=pq_json)
lock = FileLock(lock_path)
    
class SDSubmissionPage(QWizardPage):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        instructions_label = QLabel("Please select a SD card")
        layout.addWidget(instructions_label)
        header_label = QLabel(self)
        header_label.setFont(QFont("Monospace"))  
        header_label.setText("Folders should follow the format:\n{projectID}_{standAGID}_{anything}_{SS or lidar}")
        layout.addWidget(header_label)
        self.file_button = QPushButton("Select SD", self)
        self.file_button.clicked.connect(self.select_file)
        layout.addWidget(self.file_button)
        self.filename_label = QLabel("", self)
        layout.addWidget(self.filename_label)
        self.dropdown = QComboBox(self)
        self.pilot_label = QLabel("Select pilot", self)
        layout.addWidget(self.pilot_label)
        options_list = ["select a pilot", "Matthew", "Tristan", "Jake", "James"]  
        self.dropdown.addItems(options_list)
        layout.addWidget(self.dropdown)
        self.setLayout(layout)
        self.upload = None
        self.filetypes = integration.get_filetypes()

    def initializePage(self):
        self.setTitle("Pilot SD Upload")

    def parse_sd_contents(self, sd_path):
        folders = list(os.listdir(sd_path))
        contents = []
        pattern = r'^\d{6}_\d{3}_'
        for folder in folders:
            if not re.match(pattern, folder):
                continue
            folder_split = folder.split("_")
            proj_id, stand_id = folder_split[:2]
            is_strip_sample = folder_split[-1].upper() == "SS"
            row = {
                "FILETYPE": "flight_images" if not is_strip_sample else "strip_sample_images",
                "CLIENT_ID": integration.client_id_from_project_id(proj_id),
                "PROJECT_ID": proj_id,
                "STAND_ID": stand_id,
                "SOURCE": os.path.join(sd_path, folder)
            }
            contents.append(row)
        return pd.DataFrame(contents)

    def select_file(self):
        file_path = QFileDialog.getExistingDirectory(self, "Select SD")
        if not file_path:
            return
        sd_pattern = r"SD-\d{4}"
        if not re.search(sd_pattern, file_path):
            QMessageBox.critical(self, "Invalid Path", "The selected path does not follow the 'SD-1234/DCIM' convention.")
            return
        if not file_path.endswith("DCIM"):
            file_path = file_path.rstrip("/") + "/DCIM"  # Ensure there's no trailing slash before appending "DCIM"
        self.filename_label.setText(file_path)
        files = self.parse_sd_contents(file_path).fillna("")
        if 'SUB_SOURCE' not in files:
            files['SUB_SOURCE'] = ''
        path_cls = Path if is_linux else PureWindowsPath
        files["FULL"] = files.apply(lambda row: str(path_cls(row['SOURCE']) / row['SUB_SOURCE']), axis=1)
        def group_and_aggregate(df):
            grouped_df = df.groupby(['FILETYPE', 'CLIENT_ID', 'PROJECT_ID', 'STAND_ID'])['FULL'].agg(list).reset_index()
            return grouped_df
        self.upload = group_and_aggregate(files)

    def get_entries(self):
        if self.upload is None:
            return []
        entries = []
        for i, r in self.upload.iterrows():
            stand_p_id = integration.get_stand_pid_from_ids(
                r["CLIENT_ID"], r["PROJECT_ID"], r["STAND_ID"]
            )
            filetype = r["FILETYPE"].lower()
            entry = {
                "filetype": filetype,
                "CLIENT_ID": r["CLIENT_ID"], 
                "PROJECT_ID": r["PROJECT_ID"],
                "STAND_ID": r["STAND_ID"],
                "STAND_PERSISTENT_ID": stand_p_id,
                "names": [filetype],
                "files": [r["FULL"]],
                "type": [self.filetypes[filetype]["type"]] * len(r["FULL"])
            }   
            entries.append(entry)
        return entries, self.filename_label.text(), self.dropdown.currentText()
    
class FileVerificationPage(QWizardPage):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTitle("Verify File Upload Submissions")
        layout = QVBoxLayout(self)
        self.filename_label = QLabel("", self)
        layout.addWidget(self.filename_label)
        self.pilot_label = QLabel("", self)
        layout.addWidget(self.pilot_label)
        self.label = QLabel("Below are the entries for verification:", self)
        layout.addWidget(self.label)
        self.list_widget = QListWidget(self)
        layout.addWidget(self.list_widget)
        self.setLayout(layout)

    def initializePage(self):
        entries, sd_name, pilot_name = self.wizard().sd_upload_page.get_entries()
        vkeys = ["CLIENT_ID", "PROJECT_ID", "STAND_ID", "filetype", "files"]
        entries = [{k:e.get(k, None) for k in vkeys} for e in entries]
        formatted_entries = [json.dumps(e, indent=4) for e in entries]
        self.list_widget.clear()
        self.list_widget.addItems(formatted_entries)
        self.filename_label.setText("SD: " + sd_name)
        self.pilot_label.setText("Pilot: " + pilot_name)

class App(QWizard):
    def __init__(self):
        super().__init__()
        self.sd_upload_page = SDSubmissionPage()
        self.addPage(self.sd_upload_page)
        self.verify_page = FileVerificationPage()
        self.addPage(self.verify_page)
        self.setWindowTitle("Flight Data Upload Portal")
        self.finished.connect(self.on_submit)

    def on_submit(self):
        if self.result() != 1:
            dup_logger.error("CANCELLING SUBMISSION")
            return
        file_entries, sd_path, pilot = self.sd_upload_page.get_entries()
        pattern = r"SD-\d{4}"
        sd_name = re.search(pattern, sd_path).group()
        for entry in file_entries:
            entry_json = json.dumps(entry, indent=4)
            integration.update_flight_info(
                entry["CLIENT_ID"], entry["PROJECT_ID"], entry["STAND_ID"],
                [["SD_CARD", sd_name], ["PILOT", pilot], ["FLIGHT_COMPLETE", 1]]
            )
            with lock:
                uploadQ.put(entry_json)
            print(entry_json)
            dup_logger.info("Submitting file upload\n" + entry_json)
            sys.stdout.flush()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = App()
    window.show()
    sys.exit(app.exec())