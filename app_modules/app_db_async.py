import sys
import os
import asyncio
import asyncpg
from typing import Self
from time import time
import multiprocessing
import queue
from datetime import datetime

#set the module path
current = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current)

from app_log import sendToLog

class EX_db_proccess(Exception):
    """
        The exception class which is raised by the DB class on errors. Extends the base Exception Class
    """
    pass

class DBConnector:
    """ A asynchronous class for handling the DB transactions
    """
    def __init__(self, name:str=None, maxDBCon:int=None, timeout:float=None)->Self|None:
        """ Creates an instances of the class and sets the max number of connections & timeout for this instance with the PostgreSQL DB.

            :param str name:
                The name of the Instance of the class. Default value: 'DBConnector'
            :param int maxDBCon:
                The maximoum pool connection with the DB. Default value: 5
            :param float timeout:
                The timout that would be used in all DB actions. Default value: 60 (secs)

            :return self:
                An instance of DBConnector class
        """
        self.name = 'DBConnector'
        if name is not None:
            if isinstance(name, str):
                self.name = name
            else:
                ms= f'Illegal type for instance name {name} was provided.'
                raise EX_db_proccess(ms)
        self.maxDBCon = 5
        if maxDBCon is not None:
            try:
                self.maxDBCon = int(maxDBCon)
            except (Exception) as e:
                ms= f'Illegal type for maximoum connection DB connections {maxDBCon} was provided: {e}'
                raise EX_db_proccess(ms).with_traceback(e.__traceback__)
        self.dbTimeout = 60 #defaut 60 sec for timeout
        if timeout is not None:
            try:
                self.dbTimeout = float(timeout)
            except (Exception) as e:
                ms= f'Illegal type for port {timeout} for DB transactions was provided: {e}'
                raise EX_db_proccess(ms).with_traceback(e.__traceback__)
        self._dbPool = None 
        self._schema = 'public'

    async def connect(self, user:str, password:str, host:str, dbname:str, port:int=None, schema: str=None)->bool|None:
        """ Sets the connection pool with the PostgreSQL DB.

            :param str user:
                The User name for the DB Connection
            :param str password:
                The password for the DB Connection
            :param str host:
                The host which the DB is located
            :param str dbname:
                The DB name
            :param int port:
                The prot number fo the DB connection
            :param str schema:
                The DB schema

            :return bool:
              The success of the connect operation
        """
        if user is None or password is None or host is None or dbname is None:
            ms = "user, password host and dbname can not be None"
            raise EX_db_proccess(ms)
        if not isinstance(user, str):
            ms = f"Illegal type for user in database section of config. expected str got {type(user)}"
            raise EX_db_proccess(ms)
        else:
            _user = user
        if not isinstance(password, str):
            ms = f"Illegal type for password in database section of config. expected str got {type(user)}"
            raise EX_db_proccess(ms)
        else:
            _password = password
        if not isinstance(host, str):
            ms = f"Illegal type for host in database section of config. expected str got {type(user)}"
            raise EX_db_proccess(ms)
        else:
            _host = host
        if not isinstance(dbname, str):
            ms = f"Illegal type for dbname in database section of config. expected str got {type(user)}"
            raise EX_db_proccess(ms)
        else:
            _dbname = dbname
        _port = 5432
        if port is not None:
            try:
                _port = int(port)
            except (Exception) as e:
                ms= f'Illegal type for port {port} for DB was provided: {e}'
                raise EX_db_proccess(ms).with_traceback(e.__traceback__)
        if schema is not None:
            if not isinstance(schema, str):
                ms= f'schema is of wrong type expected str got {type(schema)}'
                raise EX_db_proccess(ms)
            else:
               self._schema = schema
        # connect to the PostgreSQL server
        try:
            self._dbPool = await asyncpg.create_pool(host = _host, port = _port, user = _user, password = _password, database= _dbname, timeout = self.dbTimeout)
        except (Exception) as e:
            ms = f"Error While trying to Connect to DB to get connection pool: {e}"
            raise EX_db_proccess(ms).with_traceback(e.__traceback__)
        if self._dbPool is None:
            ms = f"Error While trying to Connect to DB to get connection pool (Got None)"
            raise EX_db_proccess(ms)
        return True

    async def app_disconnect(self) -> bool:
        """ Disconnect the pool from the PostgreSQL DB.
            :return bool:
              The success or not of the disconnect operation
        """
        if self._dbPool is not None:
            try:
                await asyncio.wait_for(self._dbPool.close(), timeout=self.dbTimeout)
            except TimeoutError:
                pass #ignore it 
            except Exception as e:
                ms = f"Error While trying to disconect from DB: {e}"
                raise EX_db_proccess(ms).with_traceback(e.__traceback__)
        return True

    async def get_urls_info(self, start_url_id: int=None, end_url_id: int=None) -> list|None:
        """ Get all the polling urls from the (PostgreSQL) DB

            :param int start_url_id:
                The lower limmit (including) url_id for the get url query (None for no lower limmit)
            :param int start_url_id:
                The uper limmit (including) url_id for the get url query (None for no uper limmit)

            :return list:
                A list of dictionaries which includes the collecting url informations (None for no Result)
        """
        #check if connection was done initialy
        if self._dbPool is None:
            ms = f"Error No Connection with Db exists. Connect first"
            raise EX_db_proccess(ms)

        #validate input
        _start_url_id = -1
        if start_url_id is not None:
            try:
                _start_url_id = int(start_url_id)
            except (Exception) as e:
                ms= f'start_url_id is of wrong type expected int got {type(start_url_id)} Error {e}'
                raise EX_db_proccess(ms).with_traceback(e.__traceback__)
        _end_url_id = -1
        if end_url_id is not None:
            try:
                _end_url_id = int(end_url_id)
            except (Exception) as e:
                ms= f'end_url_id is of wrong type expected int got {type(end_url_id)} Error {e}'
                raise EX_db_proccess(ms).with_traceback(e.__traceback__)

        #Create the sql
        query = f"SELECT url_id, url, interval, regex FROM {self._schema}.urls"
        sql_selector = ' where'
        if _start_url_id > 0:
            query +=  f" where url_id >= {_start_url_id}"
            sql_selector = ' and'
        if _end_url_id > 0:
            query +=  sql_selector + f" url_id <= {_end_url_id}"

        #execute query
        res = []
        # Take a connection from the pool.
        try:
            async with self._dbPool.acquire(timeout=self.dbTimeout) as con:
            # Prepaire the sql statement .
                count = 0
                for rows in await con.fetch(query, timeout=self.dbTimeout):
                    #iterate for results
                    if not rows: #all data have been recieved
                        if count == 0:
                            return None
                            break
                    for row in rows:
                        count += 1
                        id = 1
                        if count == 1:
                            r = dict()
                            r['url_id'] = int(row)
                        elif count == 2:
                            r['url'] = str(row)
                        elif count == 3:
                            r['interval'] = int(row)
                        elif count == 4:
                            if row is not None:
                                r['regex'] = str(row)
                            else:
                                r['regex'] = ''
                            res.append(r)
                            count = 0
        except Exception as e:
            ms = f"Error While trying to Execute query: {query}"
            raise EX_db_proccess(ms).with_traceback(e.__traceback__)
        return res

    async def insert_url_stats(self, url_id:int, status:int, request_ts:float, responce_ts:float, regex:str|None) -> str|None:
        """ Execute the insert query in (PostgreSQL) DB for one url stats

            :param int url_id: 
                The url_id of the Url
            :param int status: 
                The Status of the request
            :param float request_ts: 
                The request timstamp
            :param float responce_ts:
                The responce timstamp
            :param str regex:
                The result of the regex maching proccess if one ir requiered (None if it has no application or empty if no match is found)

            :return str: 
                Status of the execution of the SQL insert stats command

        """
        #check if connection was done initialy
        if self._dbPool is None:
            ms = f"Error No Connection with Db exists. Connect first"
            raise EX_db_proccess(ms)
        #validate input
        try:
            _url_id = int(url_id)
        except (Exception) as e:
            ms= f'Illegal type for url_id {url_id} ({type({url_id})}) was provided: {e}'
            raise EX_db_proccess(ms).with_traceback(e.__traceback__)
        try:
            _status = int(status)
        except (Exception) as e:
            ms= f'Illegal type for status {status} ({type({status})}) was provided: {e}'
            raise EX_db_proccess(ms).with_traceback(e.__traceback__)       
        if regex is not None:
            if not isinstance(regex, str):
                ms= f'Illegal type for regex {regex} ({type({regex})}) was provided'
                raise EX_db_proccess(ms)
            else:
               _regex = regex
        #Create query
        _query = f"INSERT INTO {self._schema}.stats (url_id, status, request_ts, responce_ts, regex_mach) VALUES({_url_id}, {_status}, '{request_ts}', '{responce_ts}', '{regex}');"
        # Take a connection from the pool.        
        try:
            async with self._dbPool.acquire(timeout=self.dbTimeout) as con:
                #exec the insert
                await con.execute(_query, timeout=self.dbTimeout)
        except (Exception) as e:
            ms= f'Exception ocured while trying to execute query: {_query}: {str(e)}'
            raise EX_db_proccess(ms)

    async def insert_url_stats_d(self, data:dict) -> str|None:
        """ Execute the insert query in (PostgreSQL) DB for one url stats 

            :param dict data: 
                A dictionary with the data to be inserts

            :return str: 
                Status of the execution of the SQL insert stats command

        """
        #check if connection was done initialy
        if self._dbPool is None:
            ms = f"Error No Connection with Db exists. Connect first"
            raise EX_db_proccess(ms)
        #validate input
        if not isinstance(data, dict):
            ms= f'Illegal type for data {data} ({type({data})}) was provided'
            raise EX_db_proccess(ms)
        if 'url_id' not in data or 'status_code' not in data or 'request_time' not in data or 'responce_time' not in data or 'regex' not in data:
            ms= f'Mising expected key from Data {data}'
            raise EX_db_proccess(ms)
        try:
            await self.insert_url_stats(data['url_id'], data['status_code'], data['request_time'], data['responce_time'], data['regex'])
        except (EX_db_proccess) as e:
            raise EX_db_proccess(e)

    async def insert_url_stats_l(self, data:list) -> str|None:
        """ 
            Execute the insert query in (PostgreSQL) DB for one url stats 

            :param list data: 
                A list with the data to be inserts

            :return str: 
                Status of the execution of the SQL insert stats command

        """
        #check if connection was done initialy
        if self._dbPool is None:
            ms = f"Error No Connection with Db exists. Connect first"
            raise EX_db_proccess(ms)
        #validate input
        if not isinstance(data, list):
            ms= f'Illegal type for data {data} ({type({data})}) was provided'
            raise EX_db_proccess(ms)
        if len(data) != 5:
            ms= f'Illigal size of List {len(data)} (expected 5)'
            raise EX_db_proccess(ms)
        try:
            await self.insert_url_stats(data[0], data[1], data[2], data[3], data[4])
        except (EX_db_proccess) as e:
            raise EX_db_proccess(e)

    async def init_db(self, user:str) -> str|None:
        """ Init The Database Schema

            :return str: 
                Status of the execution of the last SQL command of the Creation of the DB schema

        """
        #check if connection was done initialy
        if self._dbPool is None:
            ms = f"Error No Connection with Db exists. Connect first"
            raise EX_db_proccess(ms)
        if not isinstance(user, str):
            ms = f"Illegal type for user in database section of config. expected str got {type(user)}"
            raise EX_db_proccess(ms)
        else:
            _user = user
        _query = f'''
CREATE TABLE {self._schema}.urls (
	url_id int4 NOT NULL,
	url varchar(100) NOT NULL,
	interval int4 NOT NULL,
	regex varchar(512) NULL
);
CREATE UNIQUE INDEX urls_logger_id_idx ON {self._schema}.urls USING btree (url_id);
CREATE UNIQUE INDEX urls_logger_ip_idx ON {self._schema}.urls USING btree (url);
ALTER TABLE {self._schema}.urls OWNER TO {_user};
GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE {self._schema}.urls TO {_user};

INSERT INTO public.urls (url_id, url, "interval", regex) VALUES (1, 'https://www.wikipedia.org/', 30, ''), (2, 'https://www.google.com/', 15, '');


CREATE TABLE {self._schema}.stats (
	url_id int4 NOT NULL,
	status int4 NOT NULL,
	request_ts timestamp(0) NOT NULL,
	responce_ts timestamp(0) NOT NULL,
	regex_mach varchar(515) NULL
);
CREATE UNIQUE INDEX stats_url_id_idx ON {self._schema}.stats USING btree (url_id, request_ts);
ALTER TABLE {self._schema}.stats OWNER TO {_user};
GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE {self._schema}.stats TO {_user};
        '''
        # Take a connection from the pool.
        res = None
        try:
            async with self._dbPool.acquire() as con:
                #exec the insert
                res = await con.execute(_query, timeout=self.dbTimeout)
        except (Exception) as e:
            ms= f'Exception ocured while trying to execute query: {_query}: {str(e)}'
            raise EX_db_proccess(ms)
        return res
    

def dbInsert(instancename:str, dbQueue:multiprocessing.Queue, dentry:dict, logQueue:multiprocessing.Queue, maxretrycount:int)->bool:
    """ 
    This helper Function sends data the queue of the DB Thread or prints to log/stdout if no Queue is set

        :param str instancename: 
                The task that sents the data
        :param queue dbQueue: 
                The Multiproccess Queue That is used for passing the Data to DB proccess
        :param dict dentry: 
                The Dictionary with the data to be insert
        :param queue logQueue: 
                The Multiproccess Queue That is used for passing the Data to log proccess
        :param int maxretrycount: 
                The maximoum retries to be done

        :return str: 
                Status of the execution of the SQL insert stats command

    """
    if dbQueue is not None:
        _maxret = 1
        if maxretrycount is not None:
            _maxret = maxretrycount
        
        try:
            dbQueue.put(dentry, True, 2)
        except queue.Full:
            sendToLog(logQueue, 2, instancename, f"DB Insert:: [{dentry['url_id']}, {dentry['status_code']}, {dentry['request_time']}, {dentry['responce_time']}, '{dentry['regex']}']")
            return False
        else:
            sendToLog(logQueue, 2, instancename, f"DB Insert:: [{dentry['url_id']}, {dentry['status_code']}, {dentry['request_time']}, {dentry['responce_time']}, '{dentry['regex']}']")
            return True
    sendToLog(logQueue, 3, instancename, f"DB Insert [not in DB]:: [{dentry['url_id']}, {dentry['status_code']}, {dentry['request_time']}, {dentry['responce_time']}, '{dentry['regex']}']")
    return True


def prot_db_return_callback(result):
    """
        This prosses is the Callback for DB proccess
    """
    #print(f'Loging Terminated with : {result}', flush=True)
    pass

def prot_db_error_callback(error):
    """
        This prosses is the error Callback for DB proccess
    """
    print(f'***** DB Proc Terminated got an Error: {error}', flush=True)

def run_init():
    async def init_and_test_db():
        admin_params = {'user':'admin', 'password':'adminp', 'host':'localhost', 'dbname':'postgres', 'port':5432, 'schema':'public'}
        params = {'user':'testuser', 'password':'userp', 'host':'localhost', 'dbname':'postgres', 'port':5432, 'schema':'public'}
        admindb = DBConnector()
        await admindb.connect(**admin_params)
        res = await admindb.init_db(user=params['user'])
        print(res)
        await admindb.app_disconnect()
        db = DBConnector()
        await db.connect(**params)
        res = await db.get_urls_info()
        print(res)
#       res = await db.insert_url_stats(1, 200, (time()-10), (time()-2), '') 
#       print(res)
        await db.app_disconnect()
        return
    asyncio.get_event_loop().run_until_complete(init_and_test_db())

def run_fetch():
    async def init_and_test_db():
        params = {'user':'testuser', 'password':'userp', 'host':'localhost', 'dbname':'postgres', 'port':5432, 'schema':'public'}
        db = DBConnector()
        await db.connect(**params)
        res = await db.get_urls_info()
        print(str(res))
        await db.app_disconnect()
        return
    asyncio.get_event_loop().run_until_complete(init_and_test_db())

def async_db_listen_for_inserts(dbparams, dbqueue, logqueue, runasTask):
    async def run_db_async_proc(dbparams:dict, dbqueue:multiprocessing.Queue, logqueue:multiprocessing.Queue, runasTask:bool)->bool:
        """
            This prosses Run the Asychronus DB instance for Inserts
        """
        try:
            sendToLog(logqueue, 2, 'DBProccess', f'Initilaizing DB Proccess for Insert')
            db = DBConnector()
            await db.connect(**dbparams)
        except (Exception) as e:
            ms= f'Exception ocured while trying Init DB Connection: {str(e)}'
            raise EX_db_proccess(ms)
        sendToLog(logqueue, 2, 'DBProccess', f'Ready for listening for Inserts')
        while True:
            message = dbqueue.get()
            if message is None:
                break
            try:
                print(str(message))
                res = await db.insert_url_stats_l(message) 
            except (EX_db_proccess) as e:
                sendToLog(logqueue, 3, 'DBProccess', f'Failed to Inserted Data {str(message)} in DB: {str(res)}')
                raise EX_db_proccess(e)
            else:
                sendToLog(logqueue, 1, 'DBProccess', f'Inserted Data in DB: {str(res)}')
            if runasTask:
                break
        try:
            await db.app_disconnect()
        except (Exception) as e:
            ms= f'Exception ocured while trying Init DB Connection: {str(e)}'
            raise EX_db_proccess(ms)
        sendToLog(logqueue, 2, 'DBProccess', f'Ended Succesfully')
        return True
    asyncio.get_event_loop().run_until_complete(run_db_async_proc(dbparams, dbqueue, logqueue, runasTask))

def run_insert():
    with multiprocessing.Manager() as procman:
        dbQueue = procman.Queue()
        dbpool = multiprocessing.Pool(processes=1)
        DBproc = dbpool.apply_async(async_db_listen_for_inserts, ({'user':'testuser', 'password':'userp', 'host':'localhost', 'dbname':'postgres', 'port':5432, 'schema':'public'}, dbQueue, None, False), 
                                      callback=prot_db_return_callback, error_callback=prot_db_error_callback)
        dbQueue.put([1, 200, (time()-10), (time()-2), ''])
        dbQueue.put(None)
        dbpool.close()
        # wait for all tasks to finish
        dbpool.join() 

if __name__ == '__main__':
    #run_init()
    #run_fetch()
    run_insert()
