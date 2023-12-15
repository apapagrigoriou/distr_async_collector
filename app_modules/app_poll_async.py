import sys
import os
import asyncio
import httpx
import re
import multiprocessing
import threading
import queue
from typing import Self 
from time import sleep, time
from random import randint
from datetime import datetime, timedelta

#set the module path
current = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current)
from app_log import sendToLog
from app_db_async import dbInsert

async def sendasyncQ(aQueue, name, resp, maxretrycount, logQueue):
    """ This method sends data the a async queue  
        input:
            resp:  Dict  : The responce message also valid None (for error)
    """
    loop =  asyncio.get_running_loop()
    if aQueue is None:
        loop.run_in_executor(None, sendToLog, logQueue, 3, name, f"Requested async Queue to send data is None")
    else:
        retrycount = 0
        while retrycount < maxretrycount:
            loop.run_in_executor(None, sendToLog, logQueue, 1, name, f"Sending to async Queue {str(resp)}. try: {retrycount+1} / {maxretrycount}")
            try:
                aQueue.put_nowait(resp)
            except asyncio.QueueFull:
                await asyncio.sleep(1)
                retrycount +=1
                if retrycount == maxretrycount:
                    break
            else:
                loop.run_in_executor(None, sendToLog, logQueue, 1, name, f"Data {str(resp)} send to async Queue")
                return True
        loop.run_in_executor(None, sendToLog, logQueue, 1, name, f"Data {str(resp)} failed to be send to async Queue")
    return False

def sendsyncQ(pQueue, name, resp, maxretrycount, logQueue):
    """ This method sends data the a Multi Proccess Queue  
        input:
            resp:  Dict  : The responce message also valid None (for error)
    """
    if pQueue is None:
        sendToLog(logQueue, 3, name, f"Requested async Queue to send data is None")
    else:
        retrycount = 0
        while retrycount < maxretrycount:
            sendToLog(logQueue, 1, name, f"Sending to sync Queue {str(resp)}. try: {retrycount+1} / {maxretrycount}")
            try:
                pQueue.put(resp, True, 1)
            except queue.Full:
                sleep(1)
                retrycount +=1
                if retrycount == maxretrycount:
                    break
            else:
                sendToLog(logQueue, 1, name, f"Data {str(resp)} send to sync Queue")
                return True
        sendToLog(logQueue, 1, name, f"Data {str(resp)} failed to be send to sync Queue")
    return False

class Collector:
    """
        The Collector Class which is responsible for the asychronous collection of the information from the URLs
    """
    def __init__(self, name:str, e_l:asyncio.BaseEventLoop, maxTasks:int, maxretrycount:int, defaultTimeout:int, logQueue:multiprocessing.Queue):
        """ The creator of an instance of the Collector class. Sets the number of asychroous pollong tasks  

            :param str name:
                The Name of the Collector Instance
            :param Event_loop e_l:
                The main EventLoop
            :param int maxTasks:
                The Maximum Numbber of the Polling Tasks. The More The better for perfomance
            :param int maxretrycount:
                The Maxximum Number of retries
            :param int defaultTimeout:
                The Difault timout if not one is defined in the DB for the URL
            :param int logQueue:
                The loging Queue

            :return Self:
                An instance of the collector class
        """
        #needs some validation bfore assigment
        self.loop = e_l
        self.maxretrycount = maxretrycount
        self.logQueue = logQueue
        self.defaultTimeout = defaultTimeout
        self.requestQueue = asyncio.Queue()
        self.resultQueue = asyncio.Queue()
        self.tasks = []
        self.name = name
        self.maxTasks = maxTasks

    async def _pollerT(self, name:str, urlinfo:dict)->None:
        """ This is the asychronus polling task for collectiong the information of any URL.
            it runs continously and monitors the asychronus requestQueue queue of the instance and executes the recieved requests
            it pushes the result in an asychronus queue (resultQueue). 

            :param str name:
                The name(ID) of the Poller task
            :param dict urlinfo:
                A disctionary with all the data for the request
        """
        self.loop.run_in_executor(None, sendToLog, self.logQueue, 2, f"{self.name}[{name}]" , f"Collection request is in poller: {str(urlinfo)}")
        #expand the dictionary
        url_id = urlinfo['url_id']
        request_time = urlinfo['collection_ts']
        interval = urlinfo['interval']
        timeout = interval - 1
        if timeout <= 0:
            timeout = self.defaultTimeout #if no timeout is given use default
        regex_match = urlinfo['regex']
        url = urlinfo['url']
        #calculate the sleep time which is the nessasery for the correct time for the get (added a small random for requests distribution)
        polling_sleep = request_time.timestamp() - datetime.now().timestamp() + randint(1,5)/100
        self.loop.run_in_executor(None, sendToLog, self.logQueue, 1, f"{self.name}[{name}]" , f"Sleeping time : {polling_sleep}")
        #sleep until the corect time for the request is reached
        await asyncio.sleep(polling_sleep)
        self.loop.run_in_executor(None, sendToLog, self.logQueue, 1, f"{self.name}[{name}]" , f"execget : {url} timeout:{timeout}")
        #prepair the result dict
        res = dict()
        res['url_id'] = url_id
        res['status_code'] = -1
        res['request_time'] = request_time
        res['regex'] = ''
        #execute the async get
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(url, timeout=timeout)
            except (httpx.UnsupportedProtocol, httpx.HTTPError, Exception) as e: 
                #on execption set res status to -1, log and store to DB(here different numerical Ids can be defined, depending the execption reason)
                res['responce_time'] = datetime.now()
                self.loop.run_in_executor(None, sendToLog, self.logQueue, 1, f"{self.name}[{name}]" , f"Error ({str(e)}) while fetching url {url}")
            else:
                #save initial result
                res['url_id'] = url_id
                res['status_code'] = resp.status_code
                res['request_time'] = request_time
                res['responce_time'] = datetime.now()
                res['regex'] = None
                # if regex is requested ttry to get text from the url and get the regex
                if regex_match is not None:
                    if regex_match:
                        try:
                            url_txt = resp.text
                        except Exception as e:
                            self.loop.run_in_executor(None, sendToLog, self.logQueue, 3, f"{self.name}[{name}]" , f"Error to fetch text from the url while regex s requested. ({str(e)})")
                        else:
                            if len(regex_match) > 1:
                                re_res = re.findall(regex_match, url_txt)
                            if re_res:
                                res['regex'] = " ".join(re_res)
                            else:
                                res['regex'] = ''
            #write to DB/log/res queue (interproc communication)
            return(res)

    async def _poller(self, name:str)->None:
        """ This is the asychronus polling task for collectiong the information of any URL.
            it runs continously and monitors the asychronus requestQueue queue of the instance and executes the recieved requests
            it pushes the result in an asychronus queue (resultQueue). 

            :param str name:
                The Name of the Task
        """
        while True:
            #geting the message with url to get
            message = await self.requestQueue.get()
            self.loop.run_in_executor(None, sendToLog, self.logQueue, 2, f"{self.name}[{name}]" , f"Collection request recieved: {str(message)}")
            #execute poling task
            res1 = await self._pollerT(name, message)
            #write to DB/log/res queue (interproc communication)
            try:
                res = await sendasyncQ(self.resultQueue, f"{self.name}[{name}]", res1, self.maxretrycount, self.logQueue)
            except Exception as e:
                self.loop.run_in_executor(None, sendToLog, self.logQueue, 3, f"{self.name}[{name}]" , f"Error Unable to send back the responce due to ({str(e)})")
            else:
                if res == False:
                    self.loop.run_in_executor(None, sendToLog, self.logQueue, 3, f"{self.name}[{name}]" , f"Error Unable to send back the responce")
                else:
                    self.loop.run_in_executor(None, sendToLog, self.logQueue, 1, f"{self.name}[{name}]" , f"Task reponded back the res")

    async def _poll(self):
        """ This is the asychronus polling coroutine which initiate the self.maxTasks polling tasks.
        """
        # Create tasks to process the requests queue concurrently.
        for i in range(self.maxTasks): #keeping one tase for the Queue Listener
            self.loop.run_in_executor(None, sendToLog, self.logQueue, 2, self.name, f"Adding polling task handler [{i}] to Tasklist...")
            task = asyncio.create_task(self._poller(f'poller[{i}]'))
            self.tasks.append(task)

    async def _terminate(self):
        """ This is the asychronus polling private coroutine which terminates gracefullu all the open Tasks.
        """
        self.loop.run_in_executor(None, sendToLog, self.logQueue, 1, self.name, f"Requested Collector termination")
        for task in self.tasks:
            task.cancel()
        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*(self.tasks), return_exceptions=True)
        self.loop.run_in_executor(None, sendToLog, self.logQueue, 2, self.name, f"Collector terminated")

    def init_poll(self):
        """ Create tasks to process the requests queue concurrently.
        """
        self.loop.run_until_complete(self._poll())
        sendToLog(self.logQueue, 2, self.name, f"All Task Handleres are ready to listen...")

    def terminate(self):
        """ Cancel all Tasks
        """
        self.loop.run_until_complete(self._terminate())
        sendToLog(self.logQueue, 2, self.name, f"All Task Handlers are closed...")

    async def add(self, url_id:int, collection_ts:datetime, url:str, interval:int, regex_match:str=None)->bool:
        """ This method push a url for polling in the self.requestQueue asychronus queue for assigment for polling

            :param int url_id:
                The db url id
            :param float collection_ts:
                The expected timestamb for the collection
            :param str url:
                The Url to request
            :param int interval:
                The interval of the mesurement
            :param str regex_match:
                The regex match rule for testing the site

            :return bool:
                Returns True if the url for test is pushed in the queue
        """
        message = dict()
        message['url_id'] = url_id
        message['collection_ts'] = collection_ts
        message['url'] = url
        message['interval'] = interval
        message['regex'] = regex_match
        self.loop.run_in_executor(None, sendToLog, self.logQueue, 1, self.name, f"Adding URL {message['url']} ({message['url_id']}) to Collecting Queue")
        res = await sendasyncQ(self.requestQueue, f"{self.name}", message, self.maxretrycount, self.logQueue)
        return(res)
    
def adderThread(e_l, testC):
    async def addrequest():
        print (f"!!!! AdderThread Ready")
        await testC.add(1, datetime.today() + timedelta(seconds=1), 'hts:/www.wikipedia.org/', 1, None)
        await testC.add(2, datetime.today() + timedelta(seconds=5), 'https://www.wikipedia.org/', 5, None)
        await testC.add(3, datetime.today() + timedelta(seconds=5), 'https://www.wikipedia.org/', 3, None)
    future = asyncio.run_coroutine_threadsafe(addrequest(), e_l)


def resultThread(e_l, testC):
    async def waitforresponce(e_l, testC):
        print (f"!!!! resultThread Ready")
        message = await testC.resultQueue.get()
        print (f"Got responce 1{str(message)}")
        dbInsert('waitforresponce', None, message, None, testC.maxretrycount)
        message = await testC.resultQueue.get()
        dbInsert('waitforresponce', None, message, None, testC.maxretrycount)
        print (f"Got responce 2{str(message)}")
        message = await testC.resultQueue.get()
        dbInsert('waitforresponce', None, message, None, testC.maxretrycount)
        print (f"Got responce 2{str(message)}")
        e_l.call_soon_threadsafe(e_l.stop)        
    asyncio.run_coroutine_threadsafe(waitforresponce(e_l, testC), e_l)

def asyn_test_runner(e_l):
    """
    some code for initial testing of the Collector Class

    """
    testC = Collector('testCollector 1', e_l, 2, 10, 5, None)
    testC.init_poll()
    x = threading.Thread(target=adderThread, args=(e_l, testC), name="Adder")
    x.start()
    y = threading.Thread(target=resultThread, args=(e_l, testC), name="Getter")
    y.start()
    e_l.run_forever()
    x.join()
    y.join()
    testC.terminate()

if __name__ == '__main__':
    e_l = asyncio.new_event_loop()
    asyncio.set_event_loop(e_l)   

#    asyn_proc_test_runner(e_l)
    asyn_test_runner(e_l)
