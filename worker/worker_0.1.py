import os,sys,imp,subprocess,time,shutil
import boto
from boto.s3.key import Key

from subprocess import Popen
from time import strftime
from sys import executable
from sys import platform as _platform
from settings import *
from utils import *

if _platform == "win32":
    from subprocess import CREATE_NEW_CONSOLE

os.environ['S3_USE_SIGV4'] = 'True'

#Set logging. For boto we put the severity to INFO to prevent convoluting the full log file
from logger import *
logger = logging.getLogger(__name__)
logger_boto = logging.getLogger("boto")
logger_boto.setLevel(logging.INFO)

'''
Worker class defining basic worker functionalities and variables.
Usage Commands:

#download algorithms first
algorithms = Algorithms()
algorithms.downloadAlgorithms()
    
#initialize worker
worker = Worker()
worker.cleanTemporaryFolder()
   
#Run the worker
worker.run(algorithms)
'''
class Worker(object):
	region = ""
	region_host = ""
	s3bucket = ""
	app_folder = ""
	current_queue = ""
	zip_programpath = ""
	python = ""
	storages = {}	
	connection = None
	queue = None
	local_storage_access_flag = True
	local_storage_access = False	
	
	def __init__(self):
		self.region = REGION
		self.region_host = REGION_HOST
		self.s3bucket = S3BUCKET
		self.app_folder = APP_FOLDER
		self.local_storage_access_flag = LOCAL_STORAGE_ACCESS_FLAG
		self.storages = STORAGES
		self.zip_programpath = ZIP_PROGRAMPATH
		self.python = PYTHON
		#set the current queue
		if self.ifLocalStorageAccess() and self.local_storage_access_flag:
			self.current_queue = LOCAL_QUEUE_NAME
			self.local_storage_access = True
		else:
			self.current_queue = S3_QUEUE_NAME			
		pass
	
	def ifLocalStorageAccess(self):
		for s in self.storages:
			if not (os.access(self.storages[s], os.R_OK) and os.access(self.storages[s], os.W_OK)):
				return False				
		return True 
	
	#Run the worker and continuously look for new jobs in the queue   
	def run(self, algorithms):
		print self.current_queue, self.region
		queue = initializeSQS(self.current_queue, self.region)
		if queue:
			while(True):
				self.getNextJobFromQueue(queue,algorithms)
				time.sleep(5) #Wait 5 second before starting next job
		pass
	  
	def cleanTemporaryFolder(self):
		tmpFolder = os.path.join(self.app_folder, 'data','algorithms')
		if os.path.exists(tmpFolder):
			print "deleting tmp folder ", tmpFolder
			shutil.rmtree(tmpFolder)
			return
  
	def getNextJobFromQueue(self, queue, algorithms):
	  message = queue.get_messages(1,message_attributes=['All'])
	  if len(message)==0:
		libReport(debug, "No new jobs found",True)
		#clean up
		self.cleanTemporaryFolder()
		return
	  
	  supportedAlgorithms = algorithms.getSupportedAlgorithms()
	  #Get the program that is needed from the message
	  message = message[0] 
	  while (message):
		nextMessage = None
		success = False
		messageAttrbs = readMessageAttributes(message)
		algorithm = messageAttrbs['algorithm']    
		#Check if the algorithm required by the message is on the client and validated
		if supportedAlgorithms.get(algorithm,{}).get('validated'):
		  nextMessage = None
		  nextmessageAttrbs = None      
		  #Valid algorithm, so run application      
		  success = self.runJob(messageAttrbs, nextmessageAttrbs)
		elif supportedAlgorithms.get(algorithm):
		  print "Algorithm exists but test failed"
		  print "Error in algorithm:",algorithm
		else:
		  #Algorithm does not exist and needs to be downloaded
		  algorithms.downloadAlgorithm(algorithm)
		  #Validate algorithm and add to algorithms
		  supportedAlgorithms[algorithm] = algorithms.validateAlgorithm(algorithm)
		  #If successfully validated, run application:
		  if supportedAlgorithms.get(algorithm,{}).get('validated'):
			success = self.runJob(messageAttrbs)    
		  #Delete message if it was processed successfully
		if success:
		  queue.delete_message(message)
		message = nextMessage		  
	  return supportedAlgorithms
	  
	def runJob(self, job, nextmessageAttrbs=None):
	  libReport(debug,"Running job: %s" %(job['id']),True)
	  #Input file
	  inputFilename = os.path.basename(constructLocalStorageFileName(job['input'], self.storages, job['storage']))
	  if '"' in inputFilename:
		  inputFilename = inputFilename.replace('"', "")
	  #inputFilename = os.path.basename(job['input'])
	  inputDirLocal = os.path.join(self.app_folder, 'data','algorithms',job['algorithm'],'input')
	  inputFullFilenameLocal = os.path.join(inputDirLocal,inputFilename)
	  createDirectory(inputDirLocal)
	  
	  #Output file
	  outputDirLocal = os.path.join(self.app_folder, 'data','algorithms',job['algorithm'],'output')
	  outputFilename = os.path.basename(constructLocalStorageFileName(job['output'], self.storages, job['storage']))
	  if '"' in outputFilename:
		  outputFilename = outputFilename.replace('"', "")
	  #outputFilename = os.path.basename(job['output'])
	  outputFullFilenameLocal = os.path.join(outputDirLocal, outputFilename)
	  createDirectory(outputDirLocal)
		  
	  #Create input and output tmp folder using the timestamp which will be used to extract and zip input and output files
	  timestamp = str(time.time())
	  inputTempFileLocal = os.path.join(self.app_folder, 'data','algorithms',job['algorithm'],'tmp', timestamp, 'input', os.path.splitext(inputFilename)[0])
	  outputTempFileLocal = os.path.join(self.app_folder, 'data','algorithms',job['algorithm'],'tmp', timestamp,'output', os.path.splitext(inputFilename)[0])
	  createDirectory(inputTempFileLocal)
	  createDirectory(outputTempFileLocal)
	  
	  #Download file
	  ## If LOCAL_STORAGE_ACCESS, should the file be downloaded or compute over the network?  
	  if self.local_storage_access and self.local_storage_access_flag:
		successfulDownload = downloadFileFromLocalStorage(job['input'], inputFullFilenameLocal, self.storages, job['storage'])
	  else:
		print job['input'], inputFullFilenameLocal
		successfulDownload = downloadFileFromS3(job['input'], inputFullFilenameLocal, self.s3bucket, self.region)
	  if successfulDownload:
		#Extract files from archive to tmp folder if input is zipped, otherwise  just copy it
		if job['zipped']=="True":
			print("Unzipping...", inputFullFilenameLocal, inputTempFileLocal)
			#command = r'""' + self.zip_programpath + '" x "%s" -o"%s" >' %(inputFullFilenameLocal,inputTempFileLocal) + self.app_folder + r'/7zipx.txt"'
			command = createZipCommand(self.zip_programpath, inputFullFilenameLocal, inputTempFileLocal, "x")
			os.system(command)
		else:
			libReport(debug,"Copying file from %s to %s" %(inputFullFilenameLocal,inputTempFileLocal),True)
			createDirectory(inputTempFileLocal)
			shutil.copy2(inputFullFilenameLocal,inputTempFileLocal)  
		
	  #Start process in a separate pipe
	  print [self.python,os.path.join(APP_FOLDER,'algorithms',job['algorithm'],'run.py'), inputTempFileLocal, outputTempFileLocal]
	  p = Popen([self.python,os.path.join(APP_FOLDER,'algorithms',job['algorithm'],'run.py'), inputTempFileLocal, outputTempFileLocal], stdout=subprocess.PIPE, stderr=subprocess.PIPE)	
		
	  #Save output to S3
	  libReport(debug, "Waiting for the algorithm to finish: %s" %(inputFullFilenameLocal),True)
	  out, err = p.communicate()
	  
	  #Save output/error to log file
	  fileHandle = open(os.path.join(outputTempFileLocal,'log.txt'),'w+')
	  if err=="":
		err = "None"
	  fileHandle.write("Output\n"+out+"\nError:\n"+err)
	  fileHandle.close()
	  
	  #zip the output
	  #command = r'""' + self.zip_programpath + r'" a "%s" "%s\*" >' %(outputFullFilenameLocal,outputTempFileLocal) + self.app_folder + r'/7zipa.txt"'
	  command = createZipCommand(self.zip_programpath, outputTempFileLocal, outputFullFilenameLocal, "a")
	  os.system(command)
	  
	  #Upload the output file to S3
	  if self.local_storage_access and self.local_storage_access_flag:
		uploadFileToLocalStorage(outputFullFilenameLocal,job['output'], self.storages, job['storage'])
	  else:
		uploadFileToS3(outputFullFilenameLocal,job['output'],self.s3bucket)
	  
	  #Clean up: delete local input/output/tmp folders
	  try:
		os.remove(outputFullFilenameLocal)  
		os.remove(inputFullFilenameLocal) 
		shutil.rmtree(os.path.join(self.app_folder, 'data','algorithms',job['algorithm'],'tmp', timestamp))
		pass
	  except:
		libReport(error,"Could not delete one or more of the temporary data folders")
	  libReport(info,"Running job: %s: finished." %(job['id']), True)
	  
	  return True	

class Algorithms:
	#Check if the algorithm files are all present and if the test was run successfully.
	#Files that need to be there: run.py and test.py. The program will execute
	#test.py to validate the algorithm. 
	s3bucket = ""
	app_folder = ""
	region = ""
	zip_programpath = ""
			
	def __init__(self):
		self.s3bucket = S3BUCKET
		self.app_folder = APP_FOLDER	
		self.region = REGION	
		self.zip_programpath = ZIP_PROGRAMPATH
	
	def getSupportedAlgorithms(self):
	  libReport(debug,"getting algorithms on the worker...")
	  #Update the worker software if needed
	  #updateWorkerSoftware()
	   
	  #Get all the supported algorithms on this pc
	  supportedAlgorithms = {} 
	  createDirectory(os.path.join(self.app_folder,"algorithms"))
	  for algorithm in os.listdir(os.path.join(self.app_folder,"algorithms")):
		supportedAlgorithms.setdefault(algorithm,{})
		#Get name, and check if run.py and test.py are available
		supportedAlgorithms[algorithm] = self.validateAlgorithm(algorithm)		  
	  print "Algorithms found on this client:\n",supportedAlgorithms
	  return supportedAlgorithms
	
	
	def validateAlgorithm(self, algorithmName):
		algorithm = {}
		algorithm['name'] = algorithmName
		algorithm['run.py'] = True if os.path.exists(os.path.join(APP_FOLDER,"algorithms",algorithmName,"run.py")) else False
		algorithm['test.py'] = True if os.path.exists(os.path.join(APP_FOLDER,"algorithms",algorithmName,"test.py")) else False

		#Run test module to validate algorithm
		if algorithm['run.py']==True and algorithm['test.py']==True:
			testModule = imp.load_source('test',os.path.join(self.app_folder,"algorithms",algorithmName,"test.py"))
			algorithm['validated'] = testModule.test()
		else:
			algorithm['validated'] = False		
		return algorithm	
      
	def downloadAlgorithm(self, algorithmName):
		print "Downloading algorithm:",algorithmName
		#availableAlgorithms = os.listdir(os.path.join(APP_FOLDER,"algorithms"))
		bucket = connectToS3Bucket(self.s3bucket, self.region)
		fileKey = bucket.get_key('algorithms/'+algorithmName + "/" + algorithmName+'.7z')
		if not fileKey:
			fileKey = bucket.get_key('algorithms/'+algorithmName+ "/" + algorithmName + '.zip')
		tmpFolder = os.path.join(self.app_folder,'algorithms','tmp')
		createDirectory(tmpFolder)
		if fileKey:
			timestamp = str(time.time())
			zipfileName = os.path.join(tmpFolder,algorithmName + "_" + timestamp +'.7z')
			fileKey.get_contents_to_filename(zipfileName)
			dirName = os.path.join(self.app_folder,'algorithms',algorithmName)
			createDirectory(dirName)
			os.chdir(dirName)
			#Extract files from archive to tmp folder
			#command = r'""' + ZIP_PROGRAMPATH + r'" x "%s" -aoa"' %(zipfileName)
			command = self.zip_programpath + r' x %s -aoa' %(zipfileName)
			print command
			os.system(command)
			#Only copy folder which has the algorithm name, in case algorithm was not in a proper archive structure	
			#shutil.move(os.path.join(tmpFolder,algorithmName),os.path.join(APP_FOLDER,'algorithms',algorithmName))
			os.remove(zipfileName)
			print ": done."
		pass
	  
	def downloadAlgorithms(self):
		#availableAlgorithms = os.listdir(os.path.join(APP_FOLDER,"algorithms"))
		bucket = connectToS3Bucket(self.s3bucket, self.region)
		availableAlgorithms = bucket.list("algorithms")
		for algorithm in availableAlgorithms:
			#Download algorithm from bucket to tmp folder in APP_FOLDER/algorithms
			algorithmZip = algorithm.name
			algorithmName = algorithmZip.split("/")[1]
			if (".7z" in algorithmZip) or (".zip" in algorithmZip):
				#fileKey = bucket.get_key('algorithms/'+algorithmName+ "/" + algorithmName + '.7z')
				fileKey = bucket.get_key(algorithmZip)
				print "Downloading algorithm from:",fileKey		
				if fileKey and not(os.path.exists(os.path.join(self.app_folder,'algorithms',algorithmName))):
					self.downloadAlgorithm(algorithmName)
		print ": done."
		pass  
  
if __name__ == "__main__":
  
  #download algorithms
  algorithms = Algorithms()
  algorithms.downloadAlgorithms()
    
  #initialize worker
  worker = Worker()
  worker.cleanTemporaryFolder()
   
  #Run the worker
  worker.run(algorithms)
