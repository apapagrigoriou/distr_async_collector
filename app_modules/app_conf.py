#!/bin/python3
from configparser import ConfigParser

#The exeption for handling the errors of get_conf 
class EX_get_conf(Exception):
    pass

#The exeption for handling the errors of get_application_info 
class EX_get_application_info(Exception):
    pass


def get_conf(filename, section):
    """ Returns a requested section from a config (ini) file 
        input:
            filename : str : The log file to read the ini.
            section  : str : The section of the Log file we want ro read from
        output:
            params: dict     : A dictionary which the options included in that section
    """
    # create a conf parser
    parser = ConfigParser()
    # read ini file
    try:
        parser.read(filename)
    except Exception as e:
        raise EX_get_conf(f'Unable to open the the config file:{filename}').with_traceback(e.__traceback__)
    # get requested section to dict
    sec_dict = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            sec_dict[param[0]] = param[1]
    else:
        raise EX_get_conf(f'Unable to find section {section} in config file: {filename}')
    # return dict of section
    return sec_dict


def get_application_info(filename):
    """ get the information needed for configuring the application
        input:
            filename : str : The log file to read the ini.
        output:
            app_params: dict : A dictionary which the options included the otions from sections application and database and login in the correct format
    """
    #obtain the application main configuration
    try:
        app_info = get_conf(filename, 'application')
    except EX_get_conf as error:
        raise EX_get_application_info(f'[get_application_info] Unable to get section info of section application from the config file:{filename}') from error
    for ckey in ['default_timeout_sec', 'maxretries']:
        #Error if key does not exists
        if ckey not in app_info:
            raise EX_get_application_info(f"Option '{ckey}' not found in 'application' section of config file: {filename}")
        try:
            app_info[ckey] = int(app_info[ckey])
        except ValueError as e:
            raise EX_get_application_info(f"Expected an integer value for '{ckey} but got: {app_info[ckey]}").with_traceback(e.__traceback__)

    #obtain the db connection configuration
    try:
        db_info = get_conf(filename, 'database')
    except EX_get_conf as e:
        raise EX_get_application_info(f'[get_application_info] Unable to get section info of section database from the config file:{filename}').with_traceback(e.__traceback__)
    #check if nessasery DB informations exist
    if 'schema' not in db_info or 'dbname' not in db_info or 'user' not in db_info or 'password' not in db_info or 'host' not in db_info:
        raise EX_get_application_info(f'Missing on of database, or user or password or host attributes form DB section: {filename}')
    #if no port info is given the default is assumed
    if 'port' not in db_info:
        db_info['port'] = 5432
    try:
        db_info['port'] = int(db_info['port'])
    except ValueError as e:
        raise EX_get_application_info(f"Expected integer for Database port but got : {db_info['port']}").with_traceback(e.__traceback__)
    
    #obtain the loging configuration
    try:
        log_info = get_conf(filename, 'logging')
    except EX_get_conf as e:
        raise EX_get_application_info(f'[get_application_info] Unable to get section info of section logging from the config file:{filename}').with_traceback(e.__traceback__)
    #check if nessasery DB informations exist
    if 'verbose' not in log_info or 'logfilename' not in log_info or 'logfile_size' not in log_info or 'log_history' not in log_info :
        raise EX_get_application_info(f'Missing on of database, or user or password or host attributes form DB section: {filename}')
    try:
        log_info['logfile_size'] = int(log_info['logfile_size'])
    except ValueError as e:
        raise EX_get_application_info(f"Expected integer for logfile size but got : {log_info['logfile_size']}").with_traceback(e.__traceback__)
    try:
        log_info['log_history'] = int(log_info['log_history'])
    except ValueError as e:
        raise EX_get_application_info(f"Expected integer for logfile history but got : {log_info['log_history']}").with_traceback(e.__traceback__)

    #Prepare the result
    res_collector_info = dict()
    res_collector_info['application'] = app_info
    res_collector_info['database'] = db_info
    res_collector_info['logging'] = log_info
    return res_collector_info
