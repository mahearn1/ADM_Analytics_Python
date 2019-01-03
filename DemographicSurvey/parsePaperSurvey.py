import xmltodict
import json
import os
import sys
import logging
import time
import datetime
import calendar
import cx_Oracle
import rsa
import re
import common
import paramiko
from base64 import b64encode, b64decode
from manageFtp import get_files
from manageFtp import archive_file
from manageFtp import delete_files_from_sftp
from manageFtp import check_sftp_counts
from manageFtp import moveImagesFromSFTP
 

##################
#
# CREATED DATE:  06/04/2018
# DESCRIPTION :  Parses the Caso Paper Survey XML.
#                It is intended to be used along with survey.py which parses the on-line survey.
#
# Update 11/16/2018: Changed source of primary key to the view that combine Paper& Online surveys\
#                    Start survey_instance_id at 90000000 to avoid collisions with Online surveys
#
##################
# /informatica/SURVEY/Paper



def get_next_primary_key(pCursor):
    try:
        sql = "SELECT MAX(TO_NUMBER(id)) + 1 FROM anaprd.v_demographic_survey_detail"   
        pCursor.execute(sql)

        for (theId) in pCursor:    
            pass
        if pCursor.rowcount:
            return theId[0]
        else:
            return None
               
    except Exception, err:
        logging.error('Get Prime Key Error: ' + str(err))
        return None
    
def compare_survey_file_name(pCursor, pFileName):
    try:
        sql = "SELECT 1 FROM anaprd.demographic_survey_paper_d WHERE UPPER(survey_file_name) = '" + pFileName.upper() + "' AND rownum < 2"
        pCursor.execute(sql)

        for (theId) in pCursor:    
            pass
        if pCursor.rowcount:
            return theId[0]
        else:
            return None
               
    except  Exception, err:
        logging.error('Compare_survey_file_name Error: ' + str(err))

def preventTildeOnly(pString, search=re.compile(r'[~]').search):
    if re.search('[a-zA-Z1-9]', pString) == None:
        if re.search('[~]', pString) != None:
             return False
    return True

def rows_to_dict_list(pCursor):
    sql = """SELECT agency,paper_question, paper_answer, opendata_question,opendata_answer  
             FROM demographic_survey_lov"""
    pCursor.execute(sql)
    columns = [i[0] for i in pCursor.description]
    return [dict(zip(columns, row)) for row in pCursor]

def get_open_question(pLov,pQuestion):
    for i in pLov:
        if i['PAPER_QUESTION'] == pQuestion:
            return str(i['OPENDATA_QUESTION'])
    logging.error('get_open_question Error: Question ' + pQuestion + ' not found,')
    return ''

def get_open_answer(pLov, pQuestion, pAnswer):
    optional = 'N'
    for i in pLov:
        try:
            if i['PAPER_QUESTION'] == pQuestion and i['PAPER_ANSWER'] == 'Text':
                optional = 'Y'
                # Spaces in user typed data cause multiple records to be created.
                answer = pAnswer.replace(' ','_')
                return answer,optional
            if i['PAPER_QUESTION'] == pQuestion and i['PAPER_ANSWER'] == pAnswer:
                return str(i['OPENDATA_ANSWER']),optional
        except:
            logging.error('get_open_answer Error: Ques: ' +pQuestion + ' Answer ' + pAnswer + ' not found,')
    logging.error('get_open_answer Error: Invalid Question : ' + pQuestion )
    return ''

def validateDate(pDate):
    # Check that number is in the format '01022018'
    try:
        date = datetime.datetime.strptime(pDate, '%m%d%Y')
    except:
        # If the date input is bad, return today's date
        now = datetime.datetime.now()
        return now.strftime("%m%d%Y")
    return pDate


def parse_XML():
    fileList = []
    surveyId = 0
    surveyInstanceId = 0
    surveySession = ""
    surveyStatus = ""
    surveyCount = 0
    surveyLanguage = ""
    dateStarted = ""
    answer = ""
    optionDetails = ''
    agency = ''
    batchNo = ''
    surveyDate = ''
    zipCode = ''
    language = ''
    imagePath = ''
    total_record_count = 0
    total_survey_count = 0
    lastBatchNo = ''
    archive_dir = ''
    totalAlreadyLoaded = 0
    addSurveyDetail = "INSERT INTO demographic_survey_paper_d(id,survey_number,survey_instance_id,survey_status,survey_language,question,answer, answer_is_optional, data_source, survey_file_name, image_file_name) VALUES(:id,:survey_number,:survey_instance_id,:survey_status,:survey_language,:question,:answer, :answer_is_optional, :data_source, :survey_file_name, :image_file_name)" 
   
    localDownload_dir = common.read_config_file("sftp","fileDownload_path")
    archive_dir = common.read_config_file("sftp","archive_dir")
    
    try:
        user = common.read_config_file("db","user")
        password = common.read_config_file("db","passwd")
        password = common.decryptPassword(password)
        host = common.read_config_file("db","database")
        connection = cx_Oracle.connect(user, password, host)
        cursor = connection.cursor()
    except Exception, e:
        logging.error('Cursor Error: ' + e.message )


    # SFTP info
    # Get values from the config file
    hostStr = common.read_config_file("sftp","host")
    userName = common.read_config_file("sftp","user_name")
    passWord = common.read_config_file("sftp","password")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostStr, username=userName, password=passWord)
    sftp = client.open_sftp()

    
    sequenceNumber = get_next_primary_key(cursor)

    if sequenceNumber is None:
        sequenceNumber = 1
    if surveyInstanceId is None or surveyInstanceId == 0:
        surveyInstanceId = common.get_next_survey_instance(cursor)
        
    startTime = datetime.datetime.today()
    startTimeStr = startTime.strftime("%Y%m%d %H:%M:%S")
    logging.info('Started Paper ' + startTimeStr )

    
    # Get files from Axway SFTP site
    MaxNumberOfFiles = common.read_config_file("sftp","max_number_of_files")
    moveImagesFromSFTP(sftp, MaxNumberOfFiles)
    
    
    fileCountsOK = check_sftp_counts(sftp)
    
    # Retrieve data from demographic_survey_lov
    lov = rows_to_dict_list(cursor)

    continueLoading = True
    while continueLoading:
        

        # SFTP locks if too many files in SFTP folder
        moreFileToDownload = 'Y'
        while moreFileToDownload == 'Y':
            # Clear out the files in the list
            del fileList[:]
            file_name_list = get_files(fileList, sftp, cursor, MaxNumberOfFiles)
            
            if len(file_name_list) == 0 :
                moreFileToDownload = 'N'
                continueLoading = False
                continue

            lookedupAnswer = ''
            optionalAnswer = ''

            for surveyFileName in file_name_list:
         
                if compare_survey_file_name(cursor, surveyFileName) == 1:
                    if totalAlreadyLoaded > 5:
                        continueLoading = False
                        moreFileToDownload = 'N'
                    totalAlreadyLoaded += 1
                    logging.error('File has been loaded already: ' + surveyFileName)
                else:
                    # File not previously loaded, proceed.
                    surveyInstanceId += 1
                    total_survey_count += 1

                    with open(localDownload_dir + surveyFileName) as fd:
                        doc = xmltodict.parse(fd.read(), process_namespaces=True)
                        data = json.dumps(doc)
                        surveyData = json.loads(data)
                        res = surveyData["Records"]
                        res2 = res["Record"]
                        field = res2["Field"]
                        surveyIndex = 0

                   
                        try:
                            # initilize existing values so they don't flop into the next record.
                            imagePath = ''
                            batchNo = ''
                            surveyDate = ''
                            zipCode = ''
                            agency = ''
                            optionalAnswer = ''
                            noAnswer = 'Yes'
                            
                            while field[surveyIndex]:
                                skipInsert = False
                            
                                if field[surveyIndex]['@id'] == 'None':
                                   print 'FOUND NONE'
                                
                                # Some fields have fixed tags in the XML, parse them out first
                                if field[surveyIndex]['@id'] == 'Time_Stamp':
                                    timeStamp = field[surveyIndex]['Value']
                                    skipInsert = True
                                if field[surveyIndex]['@id'] == 'BatchNo':
                                    if field[surveyIndex]['Value'] != None:
                                        batchNo = field[surveyIndex]['Value'].lstrip('0')
                                    skipInsert = True
                                if field[surveyIndex]['@id'] == 'Date':
                                    if field[surveyIndex]['Value'] != None:
                                        surveyDate = field[surveyIndex]['Value']
                                    skipInsert = True
                                if field[surveyIndex]['@id'] == 'NoAnswer':
                                    if field[surveyIndex]['Value'] == '1':
                                        noAnswer = 'No'
                                    skipInsert = True
                                if field[surveyIndex]['@id'] == 'HomeZip':
                                    zipCode = field[surveyIndex]['Value']
                                    skipInsert = True
                                if field[surveyIndex]['@id'] == 'Language':
                                    if field[surveyIndex]['Value'] != None:
                                        surveyLanguage = field[surveyIndex]['Value']
                                    skipInsert = True
                                if field[surveyIndex]['@id'] == 'Full_Image_Path':
                                    imagePath = field[surveyIndex]['Value']
                                    imagePath = imagePath.replace('\\','/')
                                    imagePath = os.path.basename(imagePath)
                                    skipInsert = True

                                #The service applied for is associated with an agency
                                #Use the LOV table to get the corresponding agency
                                for i in lov:
                                    if i['PAPER_QUESTION'] == str(field[surveyIndex]['@id']):
                                        if i['AGENCY']:
                                            agency = i['AGENCY']

                                if not skipInsert:
                                  doInsert = 'Y'
                                  # No reason to process unchecked or empty survey form fields.
                                  if field[surveyIndex]['Value'] != None and field[surveyIndex]['Value'] != '0':
                                    try:                              
                                        try:
                                            lookedupQuestion = get_open_question(lov, str(field[surveyIndex]['@id']))
                                            lookedupAnswer,optionalAnswer = get_open_answer(lov, str(field[surveyIndex]['@id']), str(field[surveyIndex]['Value']))
                                            if  preventTildeOnly(lookedupAnswer) == False:
                                                doInsert = 'N'

                                        except Exception as err:
                                            logging.error('Invalid Q/A err: '+ surveyFileName+' '+ str(err)+' ' +field[surveyIndex]['Value'])


                                        # Do not put certain question/answer pairs in the OpenData Output
                                        # These are usually the 'Other' checkboxes.  The actual data is written in.
                                        if lookedupAnswer == 'Do Not Display':
                                            doInsert = 'N'

                                        optionDetails = {
                                        'id':sequenceNumber,
                                        'survey_number':str(batchNo),
                                        'survey_instance_id':str(surveyInstanceId),
                                        'survey_status':'Complete',
                                        'survey_language':surveyLanguage,
                                        'question': lookedupQuestion ,
                                        'answer':lookedupAnswer,
                                        'answer_is_optional':optionalAnswer,
                                        'data_source':'Paper',
                                        'survey_file_name':surveyFileName,
                                        'image_file_name':imagePath
                                        }
                                           
                                    except Exception as err:
                                        logging.error('Set optionA err: '+ str(err)+' ' +field[surveyIndex]['Value'])

                                    if doInsert == 'Y':
                                        # Some answers are compound (space delimited) and need to be spilt into N records
                                        if 1 ==999:  # remove split stuff to see if needed
                                          if re.search('  ', field[surveyIndex]['Value']) > 0:
                                            for response in field[surveyIndex]['Value'].split('  '):
                                                try:
                                                    optionDetails = {
                                                    'id':sequenceNumber,
                                                    'survey_number':str(batchNo),
                                                    'survey_instance_id':surveyInstanceId,
                                                    'survey_status':'Complete',
                                                    'survey_language':surveyLanguage,
                                                    'question':field[surveyIndex]['@id'],
                                                    'answer':response,
                                                    'answer_is_optional':optionalAnswer,
                                                    'data_source':'Paper',
                                                    'survey_file_name':surveyFileName,
                                                    'image_file_name':imagePath
                                                    }
                                                except Exception as err:
                                                    logging.error('Set optionB err: '+ str(err)+' '+field[surveyIndex]['Value'])

                                                try:
                                                    common.do_insert(cursor, addSurveyDetail, optionDetails)
                                                    logging.debug('Insert A: ' + surveyFileName)
                                                    total_record_count += 1
                                                    sequenceNumber += 1
                                                except Exception as err:
                                                    logging.error('Invalid Insert: '+ str(err.message))
                                        else:
                                            
                                            if lookedupAnswer:
                                                try:
                                                    #Normal answers, not split
                                                    common.do_insert(cursor, addSurveyDetail, optionDetails)
                                                    logging.debug('Insert B: ' + surveyFileName)
                                                    total_record_count += 1
                                                    sequenceNumber += 1
                                                    lookedupAnswer = '' 
                                                except Exception as err:
                                                    logging.error('Invalid Insert: '+ str(err.message))

                                surveyIndex += 1
                                
                                if batchNo:
                                    lastBatchNo = batchNo
                        except IndexError:
                            pass
                        except  Exception, err:
                            logging.error('Parse error: '+ str(err))

                        # close file so it can be archived & deleted
                        fd.close()

                        archive_file(surveyFileName,localDownload_dir, archive_dir)
                        delete_files_from_sftp(sftp, surveyFileName, archive_dir)
                                                                    
                    # A special case:
                    #   Paper survey Zipcode
                    #   This section adds an agency question record to match the online version
                    if zipCode:
                        optionDetails = {
                           'id': sequenceNumber,
                           'survey_number':str(batchNo),
                           'survey_instance_id': surveyInstanceId,
                           'survey_status':'Complete',
                           'survey_language': surveyLanguage,
                           'question':'Home ZipCode',
                           'answer':str(zipCode[:5]),
                           'answer_is_optional':'N',
                           'data_source':'Paper',
                           'survey_file_name': surveyFileName,
                           'image_file_name': imagePath
                            
                        }
                        try:
                            common.do_insert(cursor, addSurveyDetail, optionDetails)
                        except Exception as e:
                            print 'Err '+ str(e) + ' '+ str(optionDetails)
                        sequenceNumber += 1

                    # A special case:
                    #   Paper survey does not have a question 'What NYC agency did you apply for services with today?'
                    #   This section adds an agency question record to match the online version
                    sequenceNumber += 1
                    optionDetails = {
                        'id':sequenceNumber,
                        'survey_number':str(batchNo),
                        'survey_instance_id':surveyInstanceId,
                        'survey_status':'Complete',
                        'survey_language':surveyLanguage,
                        'question':'What NYC agency did you apply for services with today?',
                        'answer':agency,
                        'answer_is_optional':'N',
                        'data_source':'Paper',
                        'survey_file_name':surveyFileName,
                        'image_file_name':imagePath
                    }

                    common.do_insert(cursor, addSurveyDetail, optionDetails)
                    sequenceNumber += 1

                    # A special case2:
                    #   Paper survey does not have a question 'Do you want to answer'
                    #   This section adds an agency question record to match the online version
                    optionDetails = {
                        'id':sequenceNumber,
                        'survey_number':str(batchNo),
                        'survey_instance_id':surveyInstanceId,
                        'survey_status':'Complete',
                        'survey_language':surveyLanguage,
                        'question':'Do you want to answer the questions?',
                        'answer':noAnswer,
                        'answer_is_optional':'N',
                        'data_source':'Paper',
                        'survey_file_name':surveyFileName,
                        'image_file_name':imagePath
                    }
                    common.do_insert(cursor, addSurveyDetail, optionDetails)
                    sequenceNumber += 1

                    surveyDate = validateDate(surveyDate)
                    
                    #Update all the records for same suvey instance with fields not found in XML until the end.
                    try:
                        updateSql = "UPDATE demographic_survey_paper_d SET survey_language = '" + surveyLanguage + "',dt_submitted = TO_DATE('" + str(surveyDate) + "','mmddyyyy'), survey_number  = '" + lastBatchNo +"', image_file_name = '" +imagePath+"' WHERE survey_instance_id = " + str(surveyInstanceId)
                    except:
                        # Run this if the survey user entered an invalid date
                        updateSql = "UPDATE demographic_survey_paper_d SET survey_language = '" + surveyLanguage + "',survey_number = '" + str(lastBatchNo) + "', image_file_name = '" +imagePath+"' WHERE survey_instance_id = " + str(surveyInstanceId)
                    try:
                        if surveyInstanceId:
                            cursor.execute(updateSql)

                    except  Exception, err:
                        logging.error('UpdateA error: '+ str(err.message))
                        logging.error('sql '+ updateSql)

                        
                    # BatchNo is one of first XML tags.  Use this to propagate batchNo to all records in a batch
                    if lastBatchNo:
                        try:
                            updateSql = """UPDATE demographic_survey_paper_d SET survey_number = '""" + lastBatchNo +"""', image_file_name = '""" + imagePath + """'  WHERE  survey_instance_id = """ + str(surveyInstanceId) + """ AND survey_number IS NULL"""
                            cursor.execute(updateSql)
                        except  Exception, err:
                            logging.error('UpdateB error: '+ str(err.message))
                cursor.execute('COMMIT')
        # Close the connection
    sftp.close()
    
    endTime = datetime.datetime.today()
    endTimeStr = endTime.strftime("%Y%m%d %H:%M:%S")
    elaspedTime = common.get_time_difference(startTime,endTime)
    logging.info('Done Paper ' + endTimeStr + ' Survey Count: ' + str(total_survey_count) +' Record Count: ' + str(total_record_count) + ' Elapsed Seconds: ' + str(elaspedTime))

                                       
def main():

    # Configure logging
    logPath = common.read_config_file("logging","path")
    logFile = common.read_config_file("logging","file_name")
    logFile = logFile + datetime.datetime.now().strftime('%Y%m%d') + '.log'
    logLevel = common.read_config_file("logging","level")
    logLevel = common.translate_logging_level(logLevel)
    logging.basicConfig(filename=logPath + '/' + logFile,level=logLevel)

    # Setup the required proxys
    linuxUser = common.read_config_file("linux","linuxUser")
    linuxPassword = common.read_config_file("linux","linuxPasswd")
    linuxPassword = common.decryptPassword(linuxPassword)
    proxy = common.read_config_file("linux","proxy")
    os.environ['https_proxy'] = 'https://' + linuxUser + ':'+ linuxPassword + '@' + proxy
    os.environ['http_proxy'] = 'http://' + linuxUser + ':'+ linuxPassword + '@' + proxy

    # Log details and start processing
    startTime = datetime.datetime.today()
    startTimeStr = startTime.strftime("%Y%m%d %H:%M:%S")
    logging.info('Started ' + startTimeStr )
    parse_XML()


if __name__ == '__main__':

    main()
