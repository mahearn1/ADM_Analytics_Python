import os
import json
import sys
import logging
import time
import datetime
import calendar
import cx_Oracle
import rsa
from base64 import b64encode, b64decode

##################
#
# CREATED BY  :  Mark Ahearn
# CREATED DATE:  06/04/2018
# DESCRIPTION :  Common routines for Demographics Surveys
#                Online and Paper version
#
##################


def timing(pModule):
    logging.info('Start ' + pModule + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def decryptPassword(pEncryptedPassword):
    with open('privateKey.pem', mode='rb') as privatefile:
        keydata = privatefile.read()
        key_priv = rsa.PrivateKey.load_pkcs1(keydata)
    try:
        decrypted = rsa.decrypt(b64decode(pEncryptedPassword), key_priv)
        return decrypted
    except Exception, err:
        logging.error('Decryption Error: ' + str(err))
        sys.exit(-1)

        
def get_time_difference(TimeStart, TimeEnd):
    timeDiff = TimeEnd - TimeStart
    return str(round(timeDiff.total_seconds() / 60, 2))        


def read_config_file(pConfigType, pConfigItem):
    try:
        with open('config.json') as json_data_file:
            data = json.load(json_data_file)
        return(data[pConfigType][pConfigItem])
    except KeyError:
        print 'Key error in config' + pConfigType + ' ' + pConfigItem
        sys.exit(-1)
    except  Exception as err:
        print 'Error in config'+ str(err)
        sys.exit(-1)
        

# config.json holds English paramters. Translate to numbers logging module is looking for.
def translate_logging_level(pLevel):
    if pLevel == None:
        return 0
    levels = {"CRITICAL": 50,
              "ERROR": 40,
              "WARNING": 30,
              "INFO": 20,
              "DEBUG": 10,
              "NOTSET": 0 }
    return levels[pLevel]

def do_insert(pCursor, pSql, pDetails):
    try:
        pCursor.execute(pSql, pDetails)
        pCursor.execute('COMMIT')
    except Exception, err:
        raise   
   

def convertDate(timestamp):
    # Format string and return EST
    try:
        dt = datetime.datetime.strptime(timestamp, "%m/%d/%Y %H:%M:%S %p")
    except Exception, err:
        try:
            dt = datetime.datetime.strptime(timestamp, "%m%d%Y")
        except Exception, err:
            logging.error('Date conversion error ' + str(error.code)+ ' ' +str(timestamp))
            dt = ''
    return dt


def get_next_survey_instance(pCursor):
    # Arbitrary number used to separate the ranges of instance_id for paper and online surveys
    # We have no control over the online ones supplied by SurveyGizmo, but assume they will
    # never exceed the arbitraryLargerThanOnlineId. This prevents collisions between the 2 survey types.
    arbitraryLargerThanOnlineId = 90000000
    try:
        sql = "SELECT MAX(TO_NUMBER(survey_instance_id)) + 1 FROM anaprd.v_demographic_survey_detail"   
        pCursor.execute(sql)
        for (theId) in pCursor:
            pass
        if theId[0] < arbitraryLargerThanOnlineId:
            return arbitraryLargerThanOnlineId
        return theId[0]
               
    except Exception, err:
        logging.error('Get Survey Instance Error: ' + str(err))


def get_max_papersurvey_date(pCursor):
    try:
        # Match date format of SFTP site ('dd Mon yyyy')
        sql = "SELECT TO_CHAR(MAX(create_dt),'dd Mon yyyy') FROM anaprd.demographic_survey_paper_d WHERE data_source = 'Paper'"   
        pCursor.execute(sql)

        for (theDate) in pCursor:    
            pass
        return theDate[0]
               
    except Exception, err:
        logging.error('Get Survey Date Error: ' + str(err))

