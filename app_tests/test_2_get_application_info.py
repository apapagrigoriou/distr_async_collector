import sys
import os
import pytest


"""
This File is the test file for the get_application_info function
"""

#Set the the parrent path of the module
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
 
# now we can import the module in the parent
# directory.
from app_modules.app_conf import get_application_info, EX_get_application_info

#[get_application_info] Test for valid input
@pytest.mark.parametrize("e,resb", [
    ('./app_tests/configs/test_config.ini', {'application' : {'default_timeout_sec':4, 'maxretries':10},
                                    'database' : {'user':'testuser', 'password':'userp', 'host':'localhost', 'port':5432, 'dbname':'postgres', 'schema':'public'},
                                    'logging' : {'verbose':'INFO', 'logfilename':'logs/collection.log', 'logfile_size':5242880, 'log_history':3}}),
])
def test_inputs(e,resb):
    assert get_application_info(e) == resb

#[get_application_info] Test that invalid inputs raises exception
@pytest.mark.parametrize("f", [
    (None),   #None Argument
    (1),   #Wrong type Argument
    ('./app_tests/configs/nonexisting.ini'),   #file not exists
    ('./app_tests/configs/empty_test_config.ini'),  #no ini file format
    ('./app_tests/configs/test_config_no_id.ini'),  #id in ini file
    ('./app_tests/configs/test_config_no_no_int_end_url_id.ini')  #no int option as expected in ini file
])
def test_get_application_info_raises_exception_on_errors(f):
    with pytest.raises(EX_get_application_info):
        get_application_info(f)
