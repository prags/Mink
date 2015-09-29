import logging, time
LOG_FILENAME = "log" + str(time.time()) + ".txt"
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,format='%(asctime)s: %(name)s - %(levelname)s: %(message)s')

debug = logging.DEBUG
error = logging.ERROR
warn = logging.WARN
info = logging.INFO
critical = logging.CRITICAL

def libReport(messageSeverity,message,onScreen=False):
	if onScreen:
		print message
	if messageSeverity==debug:
		logging.debug(message)
	elif messageSeverity == warn:
		logging.warn(message)
	elif messageSeverity == info:
		logging.info(message)
	elif messageSeverity == error:
		logging.error(message)
	elif messageSeverity == critical:
		logging.critical(message)
	else:
		logging.debug(message)


