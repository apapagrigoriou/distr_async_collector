import sys
import os
from time import time, sleep
from os.path import isfile
from getopt import getopt
import multiprocessing
import threading
import queue 
import asyncio
from datetime import datetime, timedelta

#set the module path
current = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current)

from app_modules.app_conf import get_application_info, EX_get_application_info
from app_modules.app_log import AppLogger, sendToLog, EX_log_proccess
from app_modules.app_db_async import DBConnector, EX_db_proccess, dbInsert
from app_modules.app_poll_async import Collector


def logging_return_callback(result):
    """
        This prosses is the Callback for loging proccess
    """
    #print(f'Loging Terminated with : {result}', flush=True)
    pass

def logging_error_callback(error):
    """
        This prosses is the error Callback for loging proccess
    """
    print(f'***** Loging Terminated got an Error: {error}', flush=True)

def logger_proccess(logfilename:str, logfile_size:int, log_history:int, verbose:str, logqueue:multiprocessing.Queue)->bool:
    """
        This prosses will be spawn by multiproccess Pool .
    """
    loginst = AppLogger()
    res = loginst.add_handler(logfilename, logfile_size, log_history, verbose)
    if not res:
        return False
    res = loginst.run(logqueue, False)
    return res

async def fetch_urls(params):
    """
        This prosses will fetch the Urls for proccessing
    """
    db = DBConnector()
    await db.connect(**params)
    res = await db.get_urls_info()
    await db.app_disconnect()
    return(res)

def db_return_callback(result):
    """
        This prosses is the Callback for DB inserts proccess
    """
    #print(f'DB Terminated with : {result}', flush=True)
    pass

def db_error_callback(error):
    """
        This prosses is the error Callback for DB inserts proccess
    """
    print(f'***** DB Terminated got an Error: {error}', flush=True)

def db_listen_for_inserts(dbparams, dbqueue, logqueue):
    async def run_db_async_proc(dbparams:dict, dbqueue:multiprocessing.Queue, logqueue:multiprocessing.Queue)->bool:
        """
            This prosses Run the Asychronus DB instance for Inserts
        """
        sendToLog(logqueue, 2, 'DBProccess', f'Initilaizing DB Proccess for Insert')
        try:
            db = DBConnector()
        except (Exception) as e:
            ms= f'Exception ocured while trying Init DB Connection: {str(e)}'
            raise EX_db_proccess(ms)
        await db.connect(**dbparams)
        sendToLog(logqueue, 2, 'DBProccess', f'Ready for listening for Insert requests')
        while True:
            message = dbqueue.get()
            if message is None:
                break
            try:
                await db.insert_url_stats_d(message) 
            except (EX_db_proccess) as e:
                sendToLog(logqueue, 3, 'DBProccess', f'Failed to Inserted Data {str(message)} in DB')
                raise EX_db_proccess(e)
            else:
                sendToLog(logqueue, 1, 'DBProccess', f'Inserted Data in DB')
        try:
            await db.app_disconnect()
        except (Exception) as e:
            ms= f'Exception ocured while trying Init DB Connection: {str(e)}'
            raise EX_db_proccess(ms)
        sendToLog(logqueue, 2, 'DBProccess', f'Ended Succesfully')
        return True
    asyncio.get_event_loop().run_until_complete(run_db_async_proc(dbparams, dbqueue, logqueue))

def col_return_callback(result):
    """
        This prosses is the Callback for DB inserts proccess
    """
    print(f'Collector Terminated with : {result}', flush=True)
    pass

def col_error_callback(error):
    """
        This prosses is the error Callback for DB inserts proccess
    """
    print(f'***** Collector Terminated got an Error: {error}', flush=True)

def c_scheduler(e_l, testC, urls_dict, UrlsResponceQueue):
    async def addrequest(testC, url_id, nextcollection_ts, url, interval, regex):
        res = await testC.add(url_id, nextcollection_ts, url, interval, regex)
        return(res)
    #create the Dict of the urls_ids for the collection and next collection timestamp for first time
    #init the scheduling list with the pairs if url_ids and their next request timestamb based on curent time
    curtime = datetime.today()
    UrlCollectionSchedule = {url_id: (curtime+timedelta(seconds=int(urls_dict[url_id]['interval']))) for url_id in urls_dict.keys()}
    #beggin the scheduling loop
    next_collection_urls = []
    while True:
        try:
            responce_messg = UrlsResponceQueue.get_nowait()
        except queue.Empty:
            pass
        else:
            if responce_messg is None:
                break
            else:
                UrlCollectionSchedule[responce_messg[0]] = responce_messg[1] + timedelta(seconds=int(urls_dict[responce_messg[0]]['interval']))
        if next_collection_urls:
            #load the next collection to avaiable collector proccess Queue
            url_id, nextcollection_ts = next_collection_urls.pop(0)
            #Add nessesery info to collecotrs queue 
            future = asyncio.run_coroutine_threadsafe(addrequest(testC, url_id, nextcollection_ts, urls_dict[url_id]['url'], urls_dict[url_id]['interval'], urls_dict[url_id]['regex']), e_l)
            if future.result:
                del UrlCollectionSchedule[url_id]
            else:
                next_collection_urls.insert(0, (url_id, nextcollection_ts))
            sleep(1/1000000)
        elif UrlCollectionSchedule:
            #if UrlCollectionSchedule disct has elements sort them and take those with the smaller tiemstamb for next execution
            next_collection_ts = min(UrlCollectionSchedule.values())
            next_collection_urls = [(key, value) for key, value in UrlCollectionSchedule.items() if value == next_collection_ts]
            #print (next_collection_urls)
        else:
            sleep(1/1000000)
    #break recieved exiting

def resultThread(num:int, e_l, testC, UrlsResponceQueue, dbQueue:multiprocessing.Queue, logQueue:multiprocessing.Queue, maxretrycount:int):
    async def waitforresponce(testC):
        message = await testC.resultQueue.get()
        return(message)
    while True:
        future = asyncio.run_coroutine_threadsafe(waitforresponce(testC), e_l)
        if future.result() is None:
            UrlsResponceQueue.put(None)
            break
        else:
            res = dict(future.result())
            UrlsResponceQueue.put([res['url_id'], res['request_time']])
            dbInsert(f'Getter[{num}]', dbQueue, res, logQueue, maxretrycount)

def collector_proccess(num:int, app_params:dict, numOfColTasks:int, dbQueue:multiprocessing.Queue, logQueue:multiprocessing.Queue, urls:list)->None:
    """
        The Collection: Proccess Here is where also the sheduling is done

    """
    e_l = asyncio.new_event_loop()
    asyncio.set_event_loop(e_l)   
    #init data to dict to pass in 
    urls_dict = dict() 
    for url in urls:
        urls_dict[url['url_id']] = url
    #initiate collector instance
    testC = Collector(f'testCollector [{num}]', e_l, numOfColTasks, app_params['maxretries'], app_params['default_timeout_sec'], logQueue)
    testC.init_poll()
    UrlsResponceQueue = queue.Queue()
    x = threading.Thread(target=c_scheduler, args=(e_l, testC, urls_dict, UrlsResponceQueue), name="scheduler")
    y = threading.Thread(target=resultThread, args=(num, e_l, testC, UrlsResponceQueue, dbQueue, logQueue, app_params['maxretries']), name="Getter")
    x.start()
    y.start()
    e_l.run_forever()
    x.join()
    y.join()
    testC.terminate()



def main(argv):
    """ Main Proccess 
        The collector initialy reads the configuration for the first time, and then spawns at least 3 sub proccesses 
        depending the number of cpus. 
        1. 1 proccess for logging
        2. 1 proccess for inserting Data to DB
        3. 1 (at least) poller Procceess
            On It parent Proccess after the Urls Have been retrieved from DB runs the asychronus url poller
        :params str -c <config file (ini)>: The Config File 
        :params int -t <Number or Poller Tasks>: The Poller Tasks each polling proccess has
        :params int -p <Number of Polling Proccess>: The numbe ro fpolling proccesses
    """
    #config filename
    configfile = ''
    numOfColProcs = 1
    maxnumOfColProcs = multiprocessing.cpu_count() - 3
    numOfColTasks = 2

    #first read commandline args to get the config file
    opts, args = getopt(argv,"hc:p:t:",["config=","procces=","ptasks="])
    for opt, arg in opts:
      if opt == '-h':
         print ('''async_url_collector.py -c <config file (ini)> -p <number of procceses for Collectors> -t <number of tasks per collector>''')
         sys.exit(0)
      elif opt in ("-c", "--config"):
         configfile = arg
      elif opt in ("-p", "--procces"):
        try:
            numOfColProcs = int(arg)
        except Exception as error:
            print (f'Incorrect variables as max number of collector procceses {error}.')
            print ('''async_url_collector.py -c <config file (ini)> -p <number of procceses for Collectors> -t <number of tasks per collector>''')
            sys.exit(-1)
        if numOfColProcs < 0:
            print (f'Incorrect variables as max number of collector procceses.')
            print ('''async_url_collector.py -c <config file (ini)> -p <number of procceses for Collectors> -t <number of tasks per collector>''')
            sys.exit(-1)
        if numOfColProcs > maxnumOfColProcs:
            print (f'Your System can not suport this many Collection proccesses please select a number until {maxnumOfColProcs}')
            print ('''async_url_collector.py -c <config file (ini)> -p <number of procceses for Collectors> -t <number of tasks per collector>''')
            sys.exit(-1)
      elif opt in ("-t", "--ptasks"):
        try:
            numOfColTasks = int(arg)
        except Exception as error:
            print (f'Incorrect variables as max number of Threads for collector procceses {error}.')
            print ('''async_url_collector.py -c <config file (ini)> -p <number of procceses for Collectors> -t <number of tasks per collector>''')
            sys.exit(-1)
        if numOfColTasks < 0:
            print (f'Incorrect variables as max number of Threads for collector procceses.')
            print ('''async_url_collector.py -c <config file (ini)> -p <number of procceses for Collectors> -t <number of tasks per collector>''')
            sys.exit(-1)
    if configfile == '':
        print ('Configuration filename is missing.')
        print ('''async_url_collector.py -c <config file (ini)> -p <number of procceses for Collectors> -t <number of tasks per collector>''')
        sys.exit(-1)
    if not isfile(configfile):
        print (f'Configuration file: {configfile} not found.')
        print ('''async_url_collector.py -c <config file (ini)> -p <number of procceses for Collectors> -t <number of tasks per collector>''')
        sys.exit(-1)
    #initial load of loging section of the configuration to start the loging proccess
    try:
        app_info = get_application_info(configfile)
    except EX_get_application_info as e:
        print (f'Unable to parse Configuration file {configfile}: {e}')
        print ('''async_url_collector.py -c <config file (ini)> -p <number of procceses for Collectors> -t <number of tasks per collector>''')
        sys.exit(-1)
    else:
        if app_info is None:
            print (f'Unable to parse Configuration file {configfile} or file is empty')
            print ('''async_url_collector.py -c <config file (ini)> -p <number of procceses for Collectors> -t <number of tasks per collector>''')
            sys.exit(-1)
    
    #Starting The Main Proc
    with multiprocessing.Manager() as procman:
        logQueue = procman.Queue()
        dbQueue = procman.Queue()
        procpool = multiprocessing.Pool(processes=(numOfColProcs+2))
        #starting the logging proccess
        logproc = procpool.apply_async(logger_proccess, (app_info['logging']['logfilename'], app_info['logging']['logfile_size'], app_info['logging']['log_history'], app_info['logging']['verbose'], logQueue), callback=logging_return_callback, error_callback=logging_error_callback)
        sendToLog(logQueue, 2, 'main', 'Application Started')
        #fetching the Urls
        urls = asyncio.get_event_loop().run_until_complete(fetch_urls(app_info['database']))
        #starting the save to DB proccess
        dbproc = procpool.apply_async(db_listen_for_inserts, (app_info['database'], dbQueue, logQueue), callback=db_return_callback, error_callback=db_error_callback)
        sendToLog(logQueue, 2, 'main', 'DB Inser Proccess Started')
        #prepair to split data: (this can be don smarter but no time....)
        splitsize = int(len(urls)/numOfColProcs)
        #initilize the collectors
        colprocs=[]
        for i in range(0, numOfColProcs):
            if i == numOfColProcs-1:
                colproc = procpool.apply_async(collector_proccess, (i, app_info['application'], numOfColTasks, dbQueue, logQueue, urls[i*splitsize:] ), callback=col_return_callback, error_callback=col_error_callback)
            else:
                colproc = procpool.apply_async(collector_proccess, (i, app_info['application'], numOfColTasks, dbQueue, logQueue, urls[(i*splitsize):((i+1)*splitsize)]), callback=col_return_callback, error_callback=col_error_callback)
            colprocs.append(colproc)
            sendToLog(logQueue, 2, 'main', f'Collector [{i}] Started')
        while True:
            sleep(1)
        #close
        procpool.close()
        # wait for all tasks to finish
        procpool.join() 

if __name__ == '__main__':
    main(sys.argv[1:])
