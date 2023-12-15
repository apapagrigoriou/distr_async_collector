import sys
import os
import pytest


"""
This File is the test file for the app_conf module
"""

#Set the the parrent path of the module
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
 
# import the module from the application
from app_modules.app_conf import get_conf, EX_get_conf

#[get_conf] Test for valid inputs that return correct responce (for the 3 sections application, database and logging)
@pytest.mark.parametrize("a,b,res", [
    ('./app_tests/configs/test_config.ini', 'application', {'default_timeout_sec': '4', 'maxretries': '10'}),
    ('./app_tests/configs/test_config.ini', 'database', {'user':'testuser', 'password':'userp', 'host':'localhost', 'port':'5432', 'dbname':'postgres', 'schema':'public'}),
    ('./app_tests/configs/test_config.ini', 'logging', {'verbose':'INFO', 'logfilename':'logs/collection.log', 'logfile_size':'5242880', 'log_history':'3'})
])

def test_inputs(a,b,res):
    assert get_conf(a,b) == res

#[get_conf] Test for invalid inputs that rase exceptions
@pytest.mark.parametrize("c,d", [
    (None, 'application'), #None Arguments
    ('./app_tests/configs/test_config.ini', None), #None Arguments
    (1, 'application'), #wrong type  Arguments
    ('./app_tests/configs/test_config.ini', 1), #wrong type  Arguments
    ('./app_tests/configs/nonexisting.ini', 'application'), #file not exists
    ('./app_tests/configs/empty_test_config.ini', 'application'),  # no ini file format
    ('./app_tests/configs/test_config.ini', 'nonexistingsection') # no valid section requested
])

def test_get_conf_raises_exception_on_errors(c,d):
    with pytest.raises(EX_get_conf):
        get_conf(c, d)
