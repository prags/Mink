import os,sys,imp,subprocess,time,shutil
import boto
from boto.s3.key import Key
from boto.sqs.message import Message
from settings import *
from subprocess import Popen
from time import strftime
from sys import executable
from sys import platform as _platform
import boto.sqs

 
os.environ['S3_USE_SIGV4'] = 'True'

#Set logging. For boto we put the severity to INFO to prevent convoluting the full log file
from logger import *
logger = logging.getLogger(__name__)
logger_boto = logging.getLogger("boto")
logger_boto.setLevel(logging.INFO)

'''
Identify missing outputs after mink has processed
Usage: 
'''
def missingFiles(inDir, ext1, outDir, ext2):
	inlist = [f[:f.rindex('.')] for f in os.listdir(inDir) if f.endswith(ext1)]
	outlist = [f[:f.rindex('.')] for f in os.listdir(outDir) if f.endswith(ext2)]
	common = set(inlist).intersection(set(inlist))
	missing = set(inlist).difference(set(outlist))  
	print "Missing files:", len(missing)
	return missing

'''
Convenience function to read a CSV file.
'''
def readCSV(filename, hasHeader = False, returnHeader = False):
  table = []
  f = open(filename,'r')
  csvreader = csv.reader(f)
  i = 0
  headers = [] 
  for row in csvreader:
    if not hasHeader or i > 0:
      table.append(row)  
    elif hasHeader:
      headers = row      
    i = i + 1  
  f.close()
  if not returnHeader:
    return table
  else:
    return (table, headers)
'''
Convenience function to create a directory if it does not exists
Will not fail if the directory already exists.
'''
def createDirectory(path):
  #print path, "create dir"
  if not os.path.exists(str(path)):
	os.makedirs(str(path))
  pass

'''
When a worker has finished processing a job, the output can be copied to
the local storage using this function
'''	  
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

'''
When a worker starts processing a job, the input file is copied locally 
from a local storage device using this function
'''
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

'''
This function is used to map the path correctly for the local storage.
It takes into account that user can have any drive letter in "remoteFile"
This is replaced by the appropriate drive letter as mapped in a worker 
which can access local storage device.
'''    
def constructLocalStorageFileName(remoteFile,storages, storage):	
	if storage=="s3":
		return remoteFile
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
		else:
			s= remoteFile[1:] #remove / root directory
			slashPosition = s.find("/")
			remoteFile = "".join(storages[storage.lower()], s[slashpostion+1 :]) 			
	return remoteFile

'''
Connect to a bucket in S3 and return the connection. 
'''
def connectToS3Bucket(bucketName = S3BUCKET, region = REGION, s3_max_trials=10):
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
			  
'''
Connect to SimpleQueueService and get the correct queue.
'''
def initializeSQS(queue_name, region=REGION):
	try:
		conn = boto.sqs.connect_to_region(region)
		libReport(info,"Connected to SQS: %s at host: %s." %(queue_name, region))
		return conn.get_queue(queue_name)
	except: 
		libReport(error,"Could not connect to SQS: %s at host: %s." %(queue_name, region))
		return False

'''
Upload file to S3
'''		
def uploadFileToS3(localFile,remoteFile, S3BucketName = S3BUCKET, region = REGION):
	libReport(debug, "Uploading file (%s) to S3" %(localFile))
	bucket = connectToS3Bucket(S3BucketName, region)
	k = Key(bucket)
	k.key = remoteFile
	k.set_contents_from_filename(localFile)
	libReport(debug, "Uploading file (%s) to S3 finished" %(localFile),True)
  
'''
Download file from S3
'''
def downloadFileFromS3(remoteFile,localFile,bucketName = S3BUCKET, region = REGION, overWrite=False):
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

'''	
Read and save all message attributes
'''
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
	
'''
If config file is store in s3, read the file	
'''
def readConfigFileFromS3(algoDir):
	key =  bucket.get_key(algoDir+"/config")
	if not key:
		print "config file not found"
	return False
	contents = key.get_contents_as_string()
	buf = StringIO.StringIO(contents)
	conf = ConfigParser.ConfigParser()
	conf.readfp(buf)
	sectionName = conf.sections()[0]
	config = {}
	config['algorithm_directory'] = algoDir
	config['algorithm_name'] = sectionName
	#Read the cluster size
	try:
		config['cluster_size'] = config.get(sectionName, 'cluster_size')
	except:
		config['cluster_size'] = 2

	#Read the AMI ID
	try:
		config['worker_image_id'] = config.get(sectionName, 'worker_image_id')
	except:
		print "worker_image_id not specified, can not create instances to run %s" %algoDir
		return False
	
	#Read worker_instance_type
	try:
		config['worker_instance_type'] = config.get(sectionName, 'worker_instance_type') #instance type
	except:
		config['worker_instance_type'] = 'm1.small'	
	return config  

'''
Upload files to S3, assuming all the zip files are in a folder
'''
def uploadFilesToS3FromFolder(inputFolder, algorithmName, S3BucketName = S3BUCKET, region = REGION):
	files = [f for f in os.listdir(inputFolder) if f.endswith('.7z') or f.endswith('.zip')]
	if len(files)==0:
		#look for single input files
		files = os.listdir(inputFolder)
	print "Number of files to upload:%s", len(files)
	for f in files:
		localFile = os.path.join(inputFolder,f)
		basename = os.path.basename(f)
		#Upload file to s3
		remoteFilename = 'data/algorithms/'+algorithmName+'/input/'+f
		uploadFileToS3(localFile,remoteFilename,S3BucketName)
      
      
'''
Upload files as well as submit jobs to a queue
'''	
def submitJobsWithDataUpload(inputFolder, algorithmName, queue_name=S3_QUEUE_NAME, S3BucketName = S3BUCKET, region = REGION):
	queue = initializeSQS(queue_name, region)	
	files = [f for f in os.listdir(inputFolder) if f.endswith('.7z') or f.endswith('.zip')]
	if len(files)==0:
		#look for single input files
		files = os.listdir(inputFolder)
	print "Number of files to upload:%d" %len(files)
	for f in files:
		localFile = os.path.join(inputFolder,f)
		basename = os.path.basename(f)
		#Upload file to s3
		remoteFilename = 'data/algorithms/'+algorithmName+'/input/'+f
		uploadFileToS3(localFile,remoteFilename,S3BucketName)
		outputfile = 'data/algorithms/' + algorithmName +'/output/' + os.path.splitext(basename)[0] + ".7z"
		sendSingleJobToQueue(queue, remoteFilename, outputfile, algorithmName, "s3")
		
'''
Submit local jobs without any data upload
Example options storage = "rdstorage1", "resfilsp03", "resfilsp04"
Best supported: "resfilsp04"
Usage:
selection = ['1396_16_RE_F2_LS.jpg', '1583_12_RE_F2_LS.jpg', '1583_00_RE_F2_LS.jpg', 'G068_20_RE_F2_RS.jpg']
Example: utils.submitLocalJobsFromFolder(<input folder>, <algorithm name>, "resfilsp04", <output folder>, selection)
'''		
def submitLocalJobsFromFolder(inputFolder, algorithmName, storage, outputFolder ="", selection =[], queue_name=LOCAL_QUEUE_NAME, region = REGION):
	queue = initializeSQS(queue_name, region)	
	print queue
	files = [f for f in os.listdir(inputFolder) if f.endswith('.7z') or f.endswith('.zip')]
	if len(files)==0:
		#look for single input files
		files = os.listdir(inputFolder)
	print "Number of files to upload:%d" %(len(files))
	if selection:
		files = selection		
		print "Number of files selected:%d" %(len(files))		
	for f in files:
		localFile = os.path.join(inputFolder,f)
		basename = os.path.basename(f)
		print f
		if outputFolder == "":
			outputFolder = os.path.join(inputFolder,'/output/')
		outputfile = os.path.join(outputFolder,os.path.splitext(basename)[0] + ".7z")
		print localFile, outputfile
		sendSingleJobToQueue(queue, localFile, outputfile, algorithmName, storage)

'''
Submit local jobs without any data upload, provide a csv file with <input file>,<output file>
Example options storage = "rdstorage1", "resfilsp03", "resfilsp04"
Best supported: "resfilsp04"
'''		
def submitLocalJobs(inputCSVFile, algorithmName, storage, queue_name=LOCAL_QUEUE_NAME, region = REGION):
	queue = initializeSQS(queue_name, region)	
	files = readCSV(inputCSVFile)	
	print "Number of files to upload:%s", len(files)
	for f in files:
		localFile = f[0]
		basename = os.path.basename(localFile)
		outputfile = ""
		if len(f)==0:
			outputfile = os.path.dirname(localfile) + '/output/' + os.path.splitext(basename)[0] + ".7z"
		outputfile = os.path.join(outputFolder,os.path.splitext(basename)[0]+ ".7z")
		sendSingleJobToQueue(queue, localFile, outputfile, algorithmName, storage)

'''
Submit single job to a queue
Default: S3 storage is used
'''		
def sendSingleJobToQueue(queue, inputfile, outputfile, algorithmname, storage="s3"):	
	if os.path.splitext(inputfile)[1] == ".zip" or os.path.splitext(inputfile)[1]==".7z":
		zipped = "True"
	else:
		zipped = "False"
	m = Message()
	m.message_attributes = { 
	  "algorithm" : {
		"data_type": "String",
		"string_value": algorithmname
		},		
		"input" : {
			"data_type" : "String",
			"string_value" : inputfile
		 },
		 "output" : {
			"data_type" : "String",
			"string_value" : outputfile
		 },		
		 "zipped" : {
			"data_type" : "String",
			"string_value" : zipped
		 },
		 "storage" : {
			"data_type" : "String",
			"string_value" : storage
		 }
	}
	m.set_body("tmp")
	queue.write(m)
	print "Message written"


'''
Extract processed data to a folder
Used in case of local setting when the data is stored on a local storage device
'''
def extractOutput(inputFolder, outputFolder, deleteInput=False):
	zip_programpath = ZIP_PROGRAMPATH
	files = [f for f in os.listdir(inputFolder) if f.endswith('.7z') or f.endswith('.zip')]
	for f in files:
		command  = zip_programpath + " x " +  os.path.join(inputFolder, f) + " -o" + outputFolder +  " -y -r"
		print command
		subprocess.call(command, creationflags=0x08000000) #to make sure new console doesn't pop up
		if deleteInput:
			os.remove(os.path.join(inputFolder, f))


'''
Upload Algorithm to Mink
Supports zip file upload (7z or zip)
or a Folder:
Folder Contents required: __init__.py, run.py, test.py, config.ini, all the dependencies if required
Config file to specify config
'''
def uploadAlgorithm(inputAlgorithm, algorithmName = "", configFile = "config", S3BucketName = S3BUCKET, region = REGION):
	zip_programpath = ZIP_PROGRAMPATH
	zippedAlgorithm = False
	if inputAlgorithm.endswith('.7z') or inputAlgorithm.endswith('.zip'):
		zippedAlgorithm = True  	
	print zippedAlgorithm
	if algorithmName=="":
		algorithmName = os.path.splitext(os.path.basename(inputAlgorithm))[0]
	if not zippedAlgorithm:
		print "Here"
		print os.path.join(inputAlgorithm,algorithmName+".7z")
		command = zip_programpath + r' a %s %s\* >' %(os.path.join(inputAlgorithm,algorithmName+".7z"),inputAlgorithm) + inputAlgorithm + r'/7zipa.txt'
		print command
		subprocess.call(command, creationflags=0x08000000)
		inputAlgorithm = os.path.join(inputAlgorithm,algorithmName+".7z")
	ext = os.path.splitext(inputAlgorithm)[1]
	remoteFile = "algorithms/" + algorithmName + "/" + algorithmName + ext
	print remoteFile
	uploadFileToS3(inputAlgorithm, remoteFile)

'''
Delete an algorithm from S3
'''	
def deleteAlgorithm(algorithmName, S3BucketName = S3BUCKET, region = REGION):
	bucket = connectToS3Bucket(S3BucketName, region)
	prefix_algorithm = "algorithms/" + algorithmName
	for key in bucket.list(prefix=prefix_algorithm):
		key.delete()
	print "Algorithm %s deleted successfully" %algorithmName
	
def createZipCommand(zipprogram, inputfile, outputfile, option):
	command = ""
	if _platform == "linux" or _platform == "linux2":
		if option == "a":
			command = zipprogram + r' a %s %s/* > ' %(outputfile,inputfile) + APP_FOLDER + r'/7zipa.txt'
		else:
			command = zipprogram + ' x %s -o%s > ' %(inputfile,outputfile) + APP_FOLDER + r'/7zipx.txt'
	else:
		if option == "a":
			command = r'"' + zipprogram + r' a "%s" "%s/*" > ' %(outputfile,inputfile) + APP_FOLDER + r'/7zipa.txt"'
		else:
			command = r'"' + zipprogram + ' x "%s" -o"%s" > ' %(inputfile,outputfile) + APP_FOLDER + r'/7zipx.txt"'
	print command
	return command
