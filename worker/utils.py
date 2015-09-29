import os,sys,imp,subprocess,time,shutil
import boto
from boto.s3.key import Key

from subprocess import Popen
from time import strftime
from sys import executable
from sys import platform as _platform
import boto.sqs

if _platform == "win32":
    from subprocess import CREATE_NEW_CONSOLE

os.environ['S3_USE_SIGV4'] = 'True'

#Set logging. For boto we put the severity to INFO to prevent convoluting the full log file
from logger import *
logger = logging.getLogger(__name__)
logger_boto = logging.getLogger("boto")
logger_boto.setLevel(logging.INFO)

#Convenience function to create a directory if it does not exists
#Will not fail if the directory already exists.
def createDirectory(path):
  print path, "create dir"
  if not os.path.exists(str(path)):
	os.makedirs(str(path))
  pass
	  
def uploadFileToLocalStorage(src, dst, storages, storage):
  dstFile = constructLocalStorageFileName(dst, storages, storage)
  print "Upload to", dstFile
  try:
	print src, dstFile
	createDirectory(os.path.dirname(dstFile))
	shutil.copy(src, dstFile)
	libReport(debug, "Uploading file %s to %s finished" %(src,storage),True)
  except:
	libReport(error, "Failed to copy to rdstorage")

## Should files be donwloaded to worker or loaded over the network?
def downloadFileFromLocalStorage(remoteFile, localFile, storages, storage):
  #map the file name to correct network drive letter
  print "Before:", remoteFile, localFile, storage
  remoteFile = constructLocalStorageFileName(remoteFile,storages, storage)
  print "After:", remoteFile, localFile, storage
  if os.path.exists(remoteFile):
	try:
	  shutil.copy(remoteFile, localFile)
	  libReport(debug, "Downloading file (%s) from local storage %s finished" %(remoteFile,storage),True)
	except:
	  libReport(error, "Failed to download from local storage")
	  return False
  else:
	libReport(error, "File doesn't exist: %s or couldnot be connect to the local storage" %(remoteFile))
	return False
  return True
    
def constructLocalStorageFileName(remoteFile,storages, storage):	
	for s in storages:
		positionslash = remoteFile.find("\\\\")
		if positionslash==0:
		  remoteFile.replace(r"\\" + s + r"\diag", STORAGES[s])
		#if input is given via Windows
		if ":" in remoteFile: 
			position = remoteFile.find(":")
			remoteFile = os.path.join(storages[storage.lower()], remoteFile[position+1 :])
			if '"' in remoteFile:
				remoteFile = remoteFile.replace('"', "")
				remoteFile = remoteFile.replace(r"\\", "/")
				remoteFile = remoteFile.replace("\\", "/")
		#input is via linux
		else :
			s= remoteFile[1:] #remove / root directory
			slashPosition = s.find("/")
			remoteFile = "".join(storages[storages.lower()], s[slashpostion+1 :]) 			
	return remoteFile

#Connect to a bucket in S3 and return the connection. 
def connectToS3Bucket(bucketName, region, s3_max_trials=10):
	s3_trytoconnect = 0
	while True:
		try:
			region_host = 's3.' + region + '.amazonaws.com' 
			conn = boto.connect_s3(host=region_host)
			#Pragnya: Both conn lines is needed for linux worker but I don't understand why
			conn = boto.s3.connect_to_region(region,calling_format = boto.s3.connection.OrdinaryCallingFormat())
			libReport(debug,"Connected to bucket: %s at host: %s" %(bucketName,region_host))
			return conn.get_bucket(bucketName)
		except: 
			libReport(debug,"Could not connect to bucket: %s at host: %s, trying again" %(bucketName,region_host))
			s3_trytoconnect = s3_trytoconnect+1
			if(s3_trytoconnect == s3_max_trials):
				libReport(debug,"Could not connect to bucket: %s at host: %s, quitting worker" %(bucketName,region_host))
				exit()
			  
#Connect to SimpleQueueService and get the correct queue  
def initializeSQS(queue_name, region):
	try:
		conn = boto.sqs.connect_to_region(region)
		libReport(info,"Connected to SQS: %s at host: %s." %(queue_name, region))
		return conn.get_queue(queue_name)
	except: 
		libReport(error,"Could not connect to SQS: %s at host: %s." %(queue_name, region))
		return False
		
def uploadFileToS3(localFile,remoteFile,bucketName, region):
	libReport(debug, "Uploading file (%s) to S3" %(localFile))
	bucket = connectToS3Bucket(bucketName, region)
	k = Key(bucket)
	k.key = remoteFile
	k.set_contents_from_filename(localFile)
	libReport(debug, "Uploading file (%s) to S3 finished" %(localFile),True)
  
def downloadFileFromS3(remoteFile,localFile,bucketName,region, overWrite=False):
	if os.path.exists(localFile) and not overWrite:
		libReport(debug,"File already copied")
	else:
		libReport(debug, "Downloading file (%s) from S3" %(remoteFile))
		bucket = connectToS3Bucket(bucketName, region)
		try:
		  fileKey = bucket.get_key(remoteFile)
		  fileKey.get_contents_to_filename(localFile)
		  libReport(info,"Downloading S3 file (%s) to %s finished." %(remoteFile,localFile),True)
		except:
		  libReport(error,"Could not locate file (%s) in S3 bucket: %s." %(remoteFile,bucketName),True)
		  return False
	return True
	
#Read and save all message attributes
def readMessageAttributes(message):
	attributes = {}
	attributes['storage'] = message.message_attributes['storage']['string_value'] #Bucket name
	attributes['algorithm'] = message.message_attributes['algorithm']['string_value'] #Algorithm name
	attributes['input'] = message.message_attributes['input']['string_value'] #S3InputFilename
	attributes['output'] = message.message_attributes['output']['string_value'] #S3OutputFilename
	attributes['zipped'] = message.message_attributes.get('zipped',{}).get('string_value','True') #Is input zipped
	attributes['id'] = message.id
	attributes['body'] = message.get_body()
	return attributes
