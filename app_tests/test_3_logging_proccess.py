import sys
import os
import pytest
import multiprocessing
import queue 
import time
"""
This File is the test file for the DB proccess
"""

#Set the the parrent path of the module
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from app_modules.app_log import AppLogger, run_logger_proccess, EX_log_proccess

#testing initializing parameters 
@pytest.mark.parametrize("init_params", [
    None, #none 
    {'verbose': None, 'logfilename':'logs/collection.log', 'logfile_size':'5242880','log_history':'3'}, #none a attribute
    {'verbose':12, 'logfilename':'logs/collection.log', 'logfile_size':'5242880','log_history':'3'}, #wrong type attribute
    {'verbose':'alalala', 'logfilename':'logs/collection.log', 'logfile_size':'5242880','log_history':'3'}, #wrong value attribute
    {'verbose':'INFO', 'logfilename':None, 'logfile_size':'5242880','log_history':'3'}, #none a attribute
    {'verbose':'INFO', 'logfilename':1, 'logfile_size':'5242880','log_history':'3'}, #wrong type attribute
    {'verbose':'INFO', 'logfilename':'logs/collection.log', 'logfile_size':None,'log_history':'3'}, #none a attribute
    {'verbose':'INFO', 'logfilename':'logs/collection.log', 'logfile_size':True,'log_history':'3'}, #wrong type attribute
    {'verbose':'INFO', 'logfilename':'logs/collection.log', 'logfile_size':111.1,'log_history':'3'}, #wrong type attribute
    {'verbose':'INFO', 'logfilename':'logs/collection.log', 'logfile_size':'5242880','log_history':None}, #none a attribute
    {'verbose':'INFO', 'logfilename':'logs/collection.log', 'logfile_size':'lalala','log_history':'3'}, #wrong type attribute
    {'verbose':'INFO', 'logfilename':'logs/collection.log', 'logfile_size':'5242880','log_history':True}, #wrong type attribute
    {'verbose':'INFO', 'logfilename':'logs/collection.log', 'logfile_size':'5242880','log_history':111.11}, #wrong type attribute
    {'verbose':'INFO', 'logfilename':'logs/collection.log', 'logfile_size':'5242880','log_history':'lalala'}, #wrong type attribute
    {'verbose':'INFO', 'logfilename':'logs/collection.log', 'logfile_size':'5242880','log_history':'3'}, #All OK
])


#test the initialization of the loging proceess
def test_log_proc_init(init_params):
    loginst = AppLogger()
    try:
        if not isinstance(init_params, dict):
            res = loginst.add_handler(init_params, init_params,init_params,init_params)
        else:
            res = loginst.add_handler(**init_params)
    except EX_log_proccess as e:
        print (e)
        assert True
    except Exception as exc:
        assert False


def t_logging_return_callback(result):
    global qlogres
    """
        This prosses is the Callback for loging proccess
    """
    print(f'Loging Terminated with : {result}', flush=True)
    qlogres = result

def t_logging_error_callback(error):
    global qlogres
    """
        This prosses is the error Callback for loging proccess
    """
    print(f'***** Loging Terminated got an Error: {error}', flush=True)
    qlogres = False

#testing parameters 
@pytest.mark.parametrize("opmessage,res", [
    ([1, 'proc_name', 'something something'], True),   #Correnct message
    (['proc_name', 'something something'], False),   #incorrect size
    ([None, 'proc_name', 'something something'], False),   #Icnorect type  1 arg
    (['asdasd', 'proc_name', 'something something'], False),   #Incorrenct type  1 arg
    ([-2345346667, 'proc_name', 'something something'], False),   #Incorrenct number 1 arg
    ([1, None, 'something something'], False),   #Icnorect type  2 arg
    ([1, 123, 'something something'], False),   #Icnorect type  2 arg
    ([1, 'proc_name', None], False),   #Icnorect type  3 arg
    ([1, 'proc_name', 123123], False),   #Icnorect type  3 arg
    (None, True),   #Correnct for termination
])

# Test function that uses the fixture
def test_log_proc(opmessage,res):
    global qlogres
    try:
        with multiprocessing.Manager() as procman:
            logq = procman.Queue()
            logpool = multiprocessing.Pool(processes=1)
            logproc = logpool.apply_async(run_logger_proccess, ('collection.log', 5242880, 3, 'INFO', logq, True), callback=t_logging_return_callback, error_callback=t_logging_error_callback)
            logq.put(opmessage)
            logpool.close()
            # wait for all tasks to finish
            logpool.join() 
    except EX_log_proccess as e:
        print (e)
        assert True
    except Exception as exc:
        assert False
    else:
        assert qlogres == res
