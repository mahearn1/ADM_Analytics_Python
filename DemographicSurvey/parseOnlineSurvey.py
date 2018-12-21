#!/usr/bin/env python
import json
import ast
import sys
import time
import datetime
import calendar
import requests
import logging
import ast
import os
import cx_Oracle
import rsa
from base64 import b64encode, b64decode
  
##################
#
# CREATED DATE:  01/15/2018
# DESCRIPTION :  Parses the SurveyGizmo API:
#
# Update 11/16/2018: Changed source of primary key to the view that combine Paper& Online surveys
#
##################


total_record_count = 0
totalSurveyResponses = 0
totalSurveysExpected = 0
loopEnd = 0
currentSurveyPage = 1
totalDuplicates = 0
sequenceNumber = 0
lastRunDate = None


    
def gmt_to_est(timestamp):
    # Subtract 5 hours from GMT dateTime and return EST
    noTz = timestamp[:19]
    dt = datetime.datetime.strptime(noTz, "%Y-%m-%d %H:%M:%S")
    estDate = dt - datetime.timedelta(hours=6)
    return estDate


def add_to_list(surveyNumber, surveyList):
    if surveyNumber not in surveyList:
        surveyList.append(surveyNumber)
    return surveyList


def get_time_difference(TimeStart, TimeEnd):
    timeDiff = TimeEnd - TimeStart
    return str(round(timeDiff.total_seconds() / 60, 2))  


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


def rows_to_dict_list(pCursor):
    sql = """SELECT agency,online_question, online_answer, opendata_question,opendata_answer  
             FROM demographic_survey_lov"""
    pCursor.execute(sql)
    columns = [i[0] for i in pCursor.description]
    return [dict(zip(columns, row)) for row in pCursor]


def get_open_question(pLov,pQuestion):
    for i in pLov:
        if i['ONLINE_QUESTION'] == pQuestion:
            return str(i['OPENDATA_QUESTION'])
    logging.error('get_open_question Error: Question ' + pQuestion + ' not found,')
    return pQuestion


def get_open_answer(pLov, pQuestion, pAnswer):
    optional = 'N'
    for i in pLov:
        try:
            if i['ONLINE_QUESTION'] == pQuestion and i['ONLINE_ANSWER'] == pAnswer :
                return str(i['OPENDATA_ANSWER'])
        except:
            logging.error('get_open_answer Error: Ques: ' +pQuestion + ' Answer ' + pAnswer + ' not found,')
    return pAnswer

      
def get_last_run_date(pCursor, pSurveyNumber):
    # Expecting --Format=2018-07-04+14:06:04
    startOnDate = read_config_file("surveygizmo","last_run_date")
    if startOnDate:
        return startOnDate
    else:
        ## Select dt_submitted + 6 hours to account for GMT. ( Not exact, but so what if we look back 1 extra hour during DST?) 
        query = """SELECT TO_CHAR(MAX(dt_submitted) + (6/24),'yyyy-mm-dd+HH24:mi:ss') theMax
                   FROM demographic_survey_online_d
                   WHERE survey_number = '""" + pSurveyNumber +"'"
        maxDate = ""
        pCursor.execute(query)
        maxDate = pCursor.fetchone()
        
        # The first time app is called
        if maxDate[0] is None:
            logging.fatal('Get Max Survey Date Error. Survey #: ' + pSurveyNumber)
        # Every other time       
        return  maxDate[0]


def check_for_dupes(pCursor, pSession, pSurveyTime):
    try:
        result = 0
        sql = "SELECT COUNT(*) FROM demographic_survey_online_d WHERE SURVEY_SESSION = '" + str(pSession) +"' AND DT_SUBMITTED = TO_DATE('" + str(pSurveyTime) +"','yyyy-mm-dd hh24:mi:ss') AND CREATE_DT < SYSDATE - (10/1440)" 
        pCursor.execute(sql)

        for (theCount) in pCursor:    
            pass

        if theCount[0] is None:
            return 0
        else:
            return theCount[0]
               
    except Exception, err:
        logging.error('Check Dupes Error: ' + str(err))

        
def get_next_primary_key(pCursor):
    try:
        sql = "SELECT MAX(id) + 1 FROM v_demographic_survey_detail"   
        pCursor.execute(sql)    
        theId = pCursor.fetchone()
        
        # The first time app is called
        if theId[0] is None:
            return 1
        # Every other time
        return theId[0]

    except Exception, err:
        logging.fatal('Get Prime Key Error: ' + str(err))
		

        
# config.json holds English paramters. Translate to numbers logging module is looking for.
def translate_logging_level(pLevel):
    levels = {"CRITICAL": 50,
              "ERROR": 40,
              "WARNING": 30,
              "INFO": 20,
              "DEBUG": 10,
              "NOTSET": 0 }
    return levels[pLevel]


def do_insert(pCursor, pSql, pDetails):
    global total_record_count
    try:
        pCursor.execute(pSql, pDetails)
        pCursor.execute('COMMIT')
        total_record_count += 1
    except Exception, err:
        error, = err.args
        logging.error('Insert error ' + error.code)
        logging.error('Insert error ' + error.message)        

    
def only_ascii(pString):
    import string
    printable = set(string.printable)
    return filter(lambda x: x in printable, pString).strip()

    
def replace_html(pString):
    theString = only_ascii(pString)
    html = ['<strong>','</strong>','<p align="left">','</p>','<span>','</span>','<u>','</u>','<font size="4">','</font>','<p>','</p>']
    for x in range(0,12):
        theString = theString.replace(html[x],"")
    return theString


def read_config_file(pConfigType, pConfigItem):
    try:
        with open('config.json') as json_data_file:
            data = json.load(json_data_file)
        return(data[pConfigType][pConfigItem])
    except KeyError:
        logging.error('Invalid config.json entry: ' + pConfigType +' or ' + pConfigItem)
        sys.exit(0)
    except  Exception as err:
        logging.error('Other config error' + str(err))

    
def parse_api():
    global totalSurveyResponses
    global totalSurveysExpected
    global loopEnd
    global currentSurveyPage
    global totalDuplicates
    global sequenceNumber
    global lastRunDate
    surveyId = 0
    surveyInstanceId = 0
    surveySession = ""
    surveyStatus = ""
    surveyIsTest = 0
    surveyCount = 0
    surveyDate = ""
    surveyLanguage = ""
    dateStarted = ""
    answer = ""
    lastSurveyInstanceId = 0
    theSurveyList =[]
    totalSurveyResponses = 0
    MAX_LOOPS = 50
    continueParsing = False
    STOP_PARSING = False
    cursor = None

    try:
        user = read_config_file("db","user")
        password = read_config_file("db","passwd")
        password = decryptPassword(password)
        host = read_config_file("db","database")
        connection = cx_Oracle.connect(user, password, host)
        cursor = connection.cursor()
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        logging.error('Insert Error: '+ str(error.code) + ' ' + error.message + error.context)
  
    api_token = read_config_file("surveygizmo","api_token")
    api_token_secret = read_config_file("surveygizmo","api_token_secret")
    surveyNumber = read_config_file("surveygizmo","survey_number")
    results_per_page = read_config_file("surveygizmo","results_per_page")
    maxDuplicatesAllowed = read_config_file("surveygizmo","max_dups_allowed")

    if sequenceNumber == 0:
        sequenceNumber = get_next_primary_key(cursor)
        
    # Retrieve data from demographic_survey_lov
    lov = rows_to_dict_list(cursor)

    # Used to set the Where clause of the API to return only new surveys
    if currentSurveyPage  < 2:
        lastRunDate = get_last_run_date(cursor, str(surveyNumber))

    # SurveyGizmo uses a range of indexes in their JSON. Usually 1-"max_surv_index"
    # Without this limit, code needs to loop through non-exisitent indexes looking for data
    max_index = read_config_file("surveygizmo","max_surv_index")
      
    request =  "https://restapica.surveygizmo.com/v5/survey/" + str(surveyNumber) + '/surveyresponse?page=' + str(currentSurveyPage)+'&api_token=' + api_token + "&api_token_secret=" + api_token_secret + "&resultsperpage=" + str(results_per_page) + "&filter[field][0]=date_submitted&filter[operator][0]="+ ">=" + "&filter[value][0]>=" + lastRunDate

    currentSurveyPage += 1
   
    try:
        singleSurvey = requests.get(request)
    except:
        logging.error('HTTPS Failed: '+ str(surveyNumber))
        sys.exit(-1)
      
    singleText = str(singleSurvey.text)

    # Parse data out of the survey JSON
    try:
        data = json.loads(str(singleText))
    except:
        logging.error('JSON Parse: '+ str(singleText))
        sys.exit(-1)
        
    totalJsonPages = int(data["total_pages"])
    totalSurveysExpected = int(data["total_count"])

    if totalSurveysExpected == 0:
        logging.info('Done. No new surveys found for ' + lastRunDate)
        sys.exit(0)
        

    # Do not try to process Surveys with no responses
    if totalSurveysExpected > 0:
      # Loop through every Survey Response
      if totalSurveysExpected > totalSurveyResponses:
          # Do small batchs
          loopEnd = MAX_LOOPS
          continueParsing = True
      else:
          loopEnd = totalSurveysExpected
          continueParsing = False
    
      if totalSurveyResponses > totalSurveysExpected:
          continueParsing = False

      for z in range(0, loopEnd):
        try:
            surveyData = data["data"][z]

        except IndexError:
            logging.info( 'Survey '+ str(surveyNumber) )
            break
        except Exception, err:
            cursor.close()
            connection.close()
            logging.error('Fatal Survey: '+ str(surveyNumber))
            sys.exit(0)

        surveyInstanceId = int(surveyData["id"])
        surveySession    = surveyData["session_id"]
        surveyStatus     = surveyData["status"]
        surveyIsTest     = int (surveyData["is_test_data"])
        surveyDate       = surveyData["date_submitted"]
        surveyDate       = gmt_to_est(surveyDate)
        surveyLanguage   = surveyData["language"]
        questionData     = surveyData["survey_data"]
        dateStarted      = surveyData["date_started"]
        dateStarted      = gmt_to_est(dateStarted)
        
        dupeCount = check_for_dupes(cursor,surveySession,surveyDate )
        
        if totalDuplicates > maxDuplicatesAllowed:
            logging.fatal('Too many attempts to load duplicate surveys: ' + str(surveyDate))
            return False
        
        if dupeCount > 0:
            logging.error('Attempt to rerun survey '+ str(surveySession)+ ' dated '+ str(surveyDate))
            totalDuplicates += 1
            continue
  
        if surveyIsTest == 1:
            pass
            # No special logic needed. 

        
        if surveyInstanceId != lastSurveyInstanceId :
            lastSurveyInstanceId = surveyInstanceId
            theSurveyList = add_to_list(surveyInstanceId, theSurveyList)
            totalSurveyResponses = len(theSurveyList)
            
        isOptional = 'N'

        for x in range(0,max_index):
            try:
                
                pairText = questionData[str(x)]
                questionId = int(pairText["id"])
                question = replace_html(pairText["question"]).strip()
                try:
                    answer = pairText["answer"]
                    dataSource = 'Online'
                    isOptional = 'N'
                    logging.debug('  primary_key: ' + str(sequenceNumber))
                    logging.debug('  Survey_Number: ' + str(surveyNumber))
                    logging.debug('  Survey_Status: ' + str(surveyStatus))
                    logging.debug('  Session: '+ str(surveySession))
                    logging.debug('  Survey_Instance: ' + str(surveyId))
                    logging.debug('  Is Test: ' + str(surveyIsTest))
                    logging.debug('  Date_Submitted: ' + str(surveyDate))
                    logging.debug('  Survey_Language: ' + str(surveyLanguage))
                    logging.debug('  Question_Id: ' + str(questionId))
                    logging.debug('  Question:  '+ str(question).strip())
                    logging.debug('  Answer: ' + str(answer))
                    logging.debug('  Is_Optional: ' + 'N')
                    logging.debug('  Source: ' + 'OnLine')

                    try:
                        lookedupQuestion = get_open_question(lov, str(question).strip())
                        lookedupAnswer = get_open_answer(lov, str(question).strip(), str(answer))

                    except Exception as err:
                        logging.error('Invalid Q/A err: '+ surveyFileName+' '+ str(err)+' ' +field[surveyIndex]['Value'])


                    try:
                        optionDetails = {
                        'id':sequenceNumber,
                        'survey_number':surveyNumber,
                        'survey_instance_id':surveyInstanceId,
                        'survey_session':surveySession,
                        'survey_status':surveyStatus,
                        'survey_language':surveyLanguage,
                        'dt_submitted':surveyDate,
                        'question_id':questionId,
                        'question':lookedupQuestion,
                        'answer':lookedupAnswer,
                        'answer_is_optional':isOptional,
                        'data_source':'OnLine'
                        }
                    except Exception as err:
                        logging.error('Setp err: '+ str(err))

                    addSurveyDetail = "INSERT INTO demographic_survey_online_d(id,survey_number,survey_instance_id,survey_session,survey_status,survey_language,dt_submitted,question_id,question,answer,answer_is_optional, data_source) VALUES(:id,:survey_number,:survey_instance_id,:survey_session,:survey_status,:survey_language,:dt_submitted,:question_id,:question,:answer,:answer_is_optional, :data_source)" 
                    do_insert(cursor, addSurveyDetail, optionDetails)
                    sequenceNumber += 1
                    
                except:
                    # If no answer found there must be options
                    options = pairText["options"]
                    startIndex = read_config_file("surveygizmo","start_index")
                    indexIncrement = read_config_file("surveygizmo","index_increment")
                    theAnswer = ''
                    try:
                        #The answers
                        cleanQuestion = replace_html(question.strip())
                        try:
                            theAnswer = dataX['0']["answer"].strip() 
                        except:
                            try:
                                optionText = options 
                            except:
                                logging.error('Bad index')

                            optionText = str(options)
                            optionText = optionText.replace("u'","'").replace("'","\"")
                            try:
                                try:
                                    dataX = ast.literal_eval(optionText)
                                except Exception as err:
                                    logging.error('Ast usage: ' + str(err) + ' Survey: ' +str(surveyInstanceId) + ' ' + optionText)

                                for q in range(startIndex, startIndex + indexIncrement):  
                                    try:
                                        try:
                                            # Assume optional questions have specific text.
                                            if 'write-in optional' in dataX[str(q)]['option']:
                                                # Remove nonsense text from the option
                                                optionStr = str(dataX[str(q)]["option"]).replace("(write-in optional)","Write-in: ")
                                                theAnswer = optionStr + '' + dataX[str(q)]['answer']
                                                isOptional = 'Y'
                                            else:                                          
                                                theAnswer = dataX[str(q)]['answer']
                                                isOptional = 'N'
                                        except:
                                            # No need to take any action for values of z not in the survey
                                            continue                                    

                                        lookedupQuestion = get_open_question(lov, str(question).strip())
                                        #Fix odd case of embedded \u2013
                                        theAnswer = theAnswer.replace('\\u2013 ','')

                                        optionDetails = {
                                        'id':sequenceNumber,
                                        'survey_number':surveyNumber,
                                        'survey_instance_id':surveyInstanceId,
                                        'survey_session':surveySession,
                                        'survey_status':surveyStatus,
                                        'survey_language':surveyLanguage,
                                        'dt_submitted':surveyDate,
                                        'question_id':questionId,
                                        'question':lookedupQuestion,
                                        'answer':theAnswer,
                                        'answer_is_optional':isOptional,
                                        'data_source':'OnLine'
                                        }

 
                                        logging.debug( '  primary_key: ' + str(sequenceNumber))
                                        logging.debug( '  Survey_Number; ' + str(surveyNumber))
                                        logging.debug( '  Survey_Status: ' + str(surveyStatus))
                                        logging.debug( '  Session: '+ str(surveySession))
                                        logging.debug( '  Survey_Instance: ' + str(surveyId))
                                        logging.debug( '  Is Test: ' + str(surveyIsTest))
                                        logging.debug( '  Date_Submitted: ' + str(surveyDate))
                                        logging.debug( '  Survey_Language: ' + str(surveyLanguage))
                                        logging.debug( '  Question_Id: ' + str(questionId))
                                        logging.debug( '  Question:  '+ str(cleanQuestion))
                                        logging.debug( '  Answer: ' + str(theAnswer))
                                        logging.debug( '  Is_Optional: ' + str(isOptional))
                                        logging.debug( '  Source: ' + 'OnLine')

                                        addSurveyDetail = "INSERT INTO demographic_survey_online_d(id,survey_number,survey_instance_id,survey_session,survey_status,survey_language,dt_submitted,question_id,question,answer,answer_is_optional, data_source) VALUES(:id,:survey_number,:survey_instance_id,:survey_session,:survey_status,:survey_language,:dt_submitted,:question_id,:question,:answer,:answer_is_optional, :data_source)" 
                                        do_insert(cursor, addSurveyDetail, optionDetails)
                                        sequenceNumber += 1

                                    except Exception as err:
                                        logging.error('OptionDetail Error: ' + err)
    
                            except ValueError as err:
                                logging.error('optionText error' + err)
                                                                                              

                    except Exception as e:
                        logging.error('Unexpected Error: ' + e)

            # Don't try to process json element if index not found
            except:
                pass

    cursor.close()
    connection.close()
    #Stop process when last page processsed
    if currentSurveyPage > totalJsonPages:
        return False
    return continueParsing

def main():
    global totalSurveyResponses
    global totalSurveysExpected
    
    # Configure logging
    logPath = read_config_file("logging","path")
    logFile = read_config_file("logging","file_name")
    logFile = logFile + '_online' + datetime.datetime.now().strftime('%Y%m%d')
    logLevel = read_config_file("logging","level")
    logLevel = translate_logging_level(logLevel)
    logging.basicConfig(filename=logPath + '/' + logFile,level=logLevel)

    # Setup the required proxys
    linuxUser = read_config_file("linux","linuxUser")
    linuxPassword = read_config_file("linux","linuxPasswd")
    linuxPassword = decryptPassword(linuxPassword)
    os.environ['https_proxy'] = 'https://' + linuxUser + ':'+ linuxPassword +'@bcpxy.nycnet:8080'
    os.environ['http_proxy'] = 'http://' + linuxUser + ':'+ linuxPassword +'@bcpxy.nycnet:8080'
    
    # Log details and start processing
    startTime = datetime.datetime.today()
    startTimeStr = startTime.strftime("%Y%m%d %H:%M:%S")
    logging.info('Started ' + startTimeStr )
        
    while True :
        rerunParse = parse_api()
        if rerunParse == False:
            break

    endTime = datetime.datetime.today()
    endTimeStr = endTime.strftime("%Y%m%d %H:%M:%S")
    elaspedTime = get_time_difference(startTime,endTime)
    logging.info('Done ' + str(endTimeStr) + ' Total Responses: '+ str(totalSurveyResponses) +' Record Count: ' + str(total_record_count) + ' Elapsed Seconds: ' + str(elaspedTime))

    #Final error check
    if totalSurveyResponses > totalSurveysExpected:
        logging.fatal('Surveys loaded vs expected mismatch: Expected' + str(totalSurveysExpected) + ' Loaded: '+ str(totalSurveyResponses) + '. Check start_index and'  )        
    
if __name__ == '__main__':

        main()
    

    
