import logging
from logging.handlers import RotatingFileHandler
import multiprocessing
import queue 
from datetime import datetime

class EX_log_proccess(Exception):
    """
        The exception which is raised by our logging class on errors. Extends the base Exception Class
    """
    pass

class AppLogger:
    """
        The AppLogger Class which is responsible for the consetraining an login all log messages
    """
    def __init__(self):
        """
            The AppLogger Class which is responsible for the consetraining an login all log messages
        """
        #get the ROOT logger
        self.logger = logging.getLogger('root')   
        # create console handler and set it's logging level to critical
        ch = logging.StreamHandler()
        ch.setLevel(logging.CRITICAL)
        formatter = logging.Formatter('[%(asctime)s] %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)


    def add_handler(self, logfilename:str, logfile_size:int, log_history:int, verbose:str)->bool:
        """
            The appLogger Class which is responsible for the consetraining an login all log messages
        """
        # validate logging parameters
        if not isinstance(logfilename, str):
            ms = f"Illegal type/type for logfilename {logfilename} {type(logfilename)}. logfilename must be string"
            self.logger.critical(ms)
            raise EX_log_proccess(ms)
            return False
        _logfilename = logfilename
        try:
            _logfilesize = int(logfile_size)
        except Exception:
            ms = f"Illegal value for logfile_size {logfile_size}. logfile_size must be int"
            self.logger.critical(ms)
            raise EX_log_proccess(ms)
            return False
        try:
            _log_history = int(log_history)
        except Exception:
            ms = f"Illegal value/type for log_history {log_history}. log_history must be int"
            raise EX_log_proccess(ms)
            self.logger.critical(ms)
            return False
        if not isinstance(verbose, str) :
            ms = f"Illegal type for verbose parameter {verbose}{type(verbose)}. Verbose must be string with value compatrible wwith logging module"
            raise EX_log_proccess(ms)
            self.logger.critical(ms)
            return False
        _loglevel = verbose.upper()
        _numeric_loglevel = getattr(logging, _loglevel, None)
        if not isinstance(_numeric_loglevel, int):
            ms = f"Illegal value for verbose  {_loglevel}. Verbose must on of the types \
                       of logging like info, debug, error or critical"
            raise EX_log_proccess(ms)
            self.logger.critical(ms)
            return False
        # create rotaing file handler for logging
        fh = RotatingFileHandler(filename=_logfilename, maxBytes=_logfilesize, backupCount=_log_history, encoding='utf-8')
        fh.setLevel(_numeric_loglevel)
        formatter = logging.Formatter('[%(asctime)s][ %(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.setLevel(_numeric_loglevel)
        return(True)
    
    def run(self, logQueue:queue, asTask:bool)->bool:
        """
            The Main Listening Loop
        """
        if logQueue is None:
            ms = f"Multiprocces (Manager) Queue was not provided"
            self.logger.critical(ms)
            raise EX_log_proccess(ms)
            return
        self.logger.info("Logging Procces Ready to listen.")
        while True:
            #listening for logs 
            message = logQueue.get()
            if message is None: #termination requested
                break
            if not isinstance(message, list):
                ms = f"A message of not type List recieved: {type(message)}"
                self.logger.critical(ms)
                raise EX_log_proccess(ms)
                continue
            if len(message) < 3:
                ms = f"A message with less than 3 elements recieved: {len(message)}"
                self.logger.critical(ms)
                raise EX_log_proccess(ms)
                continue
            if not isinstance(message[0], int):
                ms = f"A message with not an integer in [0] recieved: {message[0]} {type(message[0])}"
                self.logger.critical(ms)
                raise EX_log_proccess(ms)
                continue
            if not isinstance(message[1], str):
                ms =f"A message with not a str in [1] recieved: {message[1]} {type(message[1])}"
                self.logger.critical(ms)
                raise EX_log_proccess(ms)
                continue
            if not isinstance(message[2], str):
                ms = f"A message with not a str in [2] recieved: {message[2]} {type(message[1])}"
                self.logger.critical(ms)
                raise EX_log_proccess(ms)
                continue
            if int(message[0]) == 1:
                self.logger.debug(f"[{str(message[1])}] {str(message[2])}")
            elif int(message[0]) == 2:
                self.logger.info(f"[{str(message[1])}] {str(message[2])}")
            elif int(message[0]) == 3:
                self.logger.error(f"[{str(message[1])}] {str(message[2])}")
            elif int(message[0]) == 4:
                self.logger.critical(f"[{str(message[1])}] {str(message[2])}")
            else:
                ms = f"A message with not valid value (1-4) in [0] recieved: {message[0]}"
                self.logger.critical(ms)
                raise EX_log_proccess(ms)
                continue
            if asTask:
                break
        self.logger.info("Logging Procces Terminated.")
        return True

def prot_logging_return_callback(result):
    """
        This prosses is the Callback for loging proccess
    """
    #print(f'Loging Terminated with : {result}', flush=True)
    pass

def prot_logging_error_callback(error):
    """
        This prosses is the error Callback for loging proccess
    """
    print(f'***** Loging Terminated got an Error: {error}', flush=True)

def run_logger_proccess(logfilename:str, logfile_size:int, log_history:int, verbose:str, logqueue:queue, asTask:bool)->bool:
    """
        This prosses will be spawn by multiproccess Pool .
    """
    loginst = AppLogger()
    res = loginst.add_handler(logfilename, logfile_size, log_history, verbose)
    if not res:
        return False
    res = loginst.run(logqueue, asTask)
    return res

def sendToLog(logQueue, severity, proc, mssg):
    """ This helper Function just logs data or prints to std out
        input:
            logQueue: Q[List] : the Queue for the log thread (if None it prints to std out )
            severity: int     : the severity
            proc    : str     : the name of the proccess/function/thread which does the loggin
            mssg    : str     : The message to be logged or Print
    """
    if logQueue is not None:
        try:
            logQueue.put([severity, proc, mssg], True, 1)
        except queue.Full:
            pass        
        except EX_log_proccess:
            print (proc + " : " + mssg)
        else: 
            return
    else:
        print(f"[{datetime.now()}] {proc} : " + mssg)


if __name__ == '__main__':
    with multiprocessing.Manager() as procman:
        logq = procman.Queue()
        logpool = multiprocessing.Pool(processes=1)
        logproc = logpool.apply_async(run_logger_proccess, ('collection.log', 5242880, 3, 'INFO', logq, False), callback=prot_logging_return_callback, error_callback=prot_logging_error_callback)
        sendToLog(logq, 2, 'proc_name2', 'something something')
        sendToLog(logq, 2, 'proc_name1', 'something something')
        logq.put(None)
        logpool.close()
        # wait for all tasks to finish
        logpool.join() 
   
