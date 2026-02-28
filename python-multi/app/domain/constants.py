import os
import re


CSV_SEP = ";"
APP_DISPLAY_NAME = "DICOM Multi Toolkit"

DCM4CHE_STORE_RQ_RE = re.compile(
    r"<<\s+\d+:C-STORE-RQ\[[\s\S]*?iuid=([0-9]+(?:\.[0-9]+)+)\s+-",
    re.IGNORECASE,
)
DCM4CHE_STORE_RSP_OK_RE = re.compile(
    r">>\s+\d+:C-STORE-RSP\[[\s\S]*?status=0H[\s\S]*?iuid=([0-9]+(?:\.[0-9]+)+)\s+-",
    re.IGNORECASE,
)
DCM4CHE_STORE_RSP_ERR_RE = re.compile(
    r">>\s+\d+:C-STORE-RSP\[[\s\S]*?status=(?!0H)([A-F0-9]+H)[\s\S]*?iuid=([0-9]+(?:\.[0-9]+)+)\s+-",
    re.IGNORECASE,
)

DCMTK_SENDING_FILE_RE = re.compile(r"I:\s+Sending file:\s+(.+)$")
DCMTK_BAD_FILE_RE = re.compile(r"E:\s+Bad DICOM file:\s+(.+?):\s*(.+)$")
DCMTK_STORE_RSP_RE = re.compile(r"I:\s+Received Store Response\s+\((.+)\)$")
DCMTK_NO_SOP_UID_RE = re.compile(r"E:\s+No SOP Class or Instance UID in file:\s+(.+)$")
DCMTK_STORE_FAILED_FILE_RE = re.compile(r"E:\s+Store Failed,\s*file:\s+(.+?):\s*$")
DCMTK_STORE_FAILED_REASON_RE = re.compile(r"E:\s+([0-9A-F]{4}:[0-9A-F]{4}\s+.+)$", re.IGNORECASE)
UID_TAG_0008_0018 = re.compile(r"\(0008,0018\)[^\[]*\[([^\]]*)\]", re.IGNORECASE)
UID_TAG_0002_0010 = re.compile(r"\(0002,0010\)[^\[]*\[([^\]]*)\]", re.IGNORECASE)
UID_VALUE_RE = re.compile(r"[0-9]+(?:\.[0-9]+)+")
IS_WINDOWS = os.name == "nt"
WINDOWS_CMD_SAFE_MAX_CHARS = 7600
WINDOWS_DIRECT_SAFE_MAX_CHARS = 30000
DCM4CHE_JAVA_MAIN_CLASS = "org.dcm4che3.tool.storescu.StoreSCU"
DCM4CHE_CRITICAL_JAR_MARKERS = [
    "dcm4che-tool-storescu",
    "dcm4che-tool-common",
    "dcm4che-net",
    "dcm4che-core",
]
