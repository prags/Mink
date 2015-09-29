from sys import platform as _platform

REGION = 'eu-west-1'
REGION_HOST = 's3.' + REGION + '.amazonaws.com' 
S3BUCKET = 'mink-server'
S3_QUEUE_NAME = "MINK_QUEUE"
LOCAL_QUEUE_NAME = "LOCALSTORAGE_QUEUE"
LOCAL_STORAGE_ACCESS=False
LOCAL_STORAGE_ACCESS_FLAG = False

S3_MAX_TRIALS = 10
ZIP_PROGRAMPATH = ""
PYTHON = ""
STORAGES = {}
APP_FOLDER = ""
CURRENT_QUEUE = ""
WORKER_PLATFORM = ""

if _platform == "linux" or _platform == "linux2":
	APP_FOLDER = "/home/mink/mink"
	PYTHON = "python"
	ZIP_PROGRAMPATH = "7z"
	STORAGES = {"resfilsp03":"/resfilsp03/", "resfilsp04":"/resfilsp04/"}
	LOCAL_QUEUE_NAME = "LOCALSTORAGE_QUEUE_LINUX"
	WORKER_PLATFORM = "LINUX"
elif _platform == "win32":
    APP_FOLDER = r"C:\mink"
    PYTHON = r"c:\Python27\python.exe"
    ZIP_PROGRAMPATH = r'"C:\Program Files\7-Zip\7z.exe"'
    STORAGES = {"rdstorage1":"R:","resfilsp03":"S:", "resfilsp04":"Z:"}
    WORKER_PLATFORM = "WINDOWS"

CREATE_NO_WINDOW = 0x08000000
