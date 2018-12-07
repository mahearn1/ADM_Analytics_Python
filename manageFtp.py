import os
import shutil
import re
import sys
import paramiko
import common  
import glob
import logging
import os.path
from os import path
from datetime import datetime

##################
#
# CREATED DATE:  07/06/2018
# DESCRIPTION :  Intended to pull files from an SFTP site.
#                Reads config, checks SFTP site, downloads files
#
##################

fileList = []
imageFileList = []


def archive_file(pFileName, pLocalDir, pArchiveDir):
    # Moves files from the working directoy to Archive
    # Both directories are define in config.json
    try:
        shutil.copy2(pLocalDir + pFileName, pArchiveDir)
        if path.exists(pArchiveDir + pFileName):
            try:
                os.remove(pLocalDir + pFileName)
            except  Exception as e:
                logging.error('Error deleting: ' + pLocalDir + pFileName + str(e))
    except Exception as e:
        logging.error('archiveFile error: ' + str(e))
    
    
def get_file_from_sftp(pSrv, pFileName, pLastModified, pFileDownloadDir, pLatestSurveyDate, pPaperSurveyExtension, pImageDownloadDir, pImageExtension, pLoadWithoutImages):
    global fileList
    global imageFileList
    try:    
        archive_dir = common.read_config_file("sftp","archive_dir")
        file_name = pFileName
        
        if pLastModified <= pLatestSurveyDate:
            #Only pull XML files with right extention
            xmlFileFound = re.search(".*\."+ pPaperSurveyExtension + "$", file_name)
            if xmlFileFound:
                if os.path.isfile(pFileDownloadDir + file_name):
                    logging.error('File ' + file_name +' already processed.')
                else:          
                    fileList.append(file_name)
                    pSrv.get(file_name, pFileDownloadDir + file_name)
                    logging.info('Downloaded: ' + file_name)

            if pLoadWithoutImages != 'Yes':
                #Only pull image files with right extention
                imageFileFound = re.search(".*\."+ pImageExtension + "$", file_name)
                if imageFileFound:
                    if os.path.isfile(pImageDownloadDir + file_name):
                        logging.error('Image ' + file_name +' already processed.')
                    else:
                        imageFileList.append(file_name)
                        pSrv.get(file_name, pImageDownloadDir + file_name)
                        logging.info('Downloaded: ' + file_name)
            
    except ValueError:
        logging.error('getFileFromFTP invalid date: ' + str(pFtpObject.st_mtime))


def get_files(pCursor):
    global fileList

    # Find current max survey date    
    max_survey_date = common.get_max_papersurvey_date(pCursor)

    # Get values from the config file
    hostStr = common.read_config_file("sftp","host")
    userName = common.read_config_file("sftp","user_name")
    passWord = common.read_config_file("sftp","password")
    ftp_dir = common.read_config_file("sftp","ftp_dir")
    localDownload_dir = common.read_config_file("sftp","fileDownload_path")
    paperSurveyExtension = common.read_config_file("sftp","file_extension")
    imageExtension = common.read_config_file("sftp","image_extension")
    imageDownloadDir = common.read_config_file("sftp","imageDownload_path")
    rerunDate = common.read_config_file("sftp","survey_date") # Used to reload survey from a specific date
    loadFileWithoutImages = common.read_config_file("sftp","load_without_images")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostStr, username=userName, password=passWord)
    sftp = client.open_sftp()
    
    # Change to the specified FTP directory
    try:
        sftp.chdir(ftp_dir)
    except:
        logging.error('Invalid FTP path: '+ ftp_dir)
        sys.exit(-1)
    rerunSurveyDate = datetime.strptime(rerunDate, '%d %b %Y')
    
    if rerunDate:
        # Use config file to set date to pull files
        try:
            rerunSurveyDate = datetime.strptime(rerunDate, '%d %b %Y')
        except:
            logging.error('Invalid survey date in config: '+ rerunDate)
    else:
        # No entry in config.  Use last survey date as starting point
        rerunSurveyDate = datetime.strptime(max_survey_date, '%d %b %Y')

    latestSurveyDate = rerunSurveyDate.strftime('%Y%m%d')
    
    # List the files
    for fileName in sftp.listdir(ftp_dir):
        #Process each file, get modified date
        utime = sftp.stat(fileName).st_mtime
        last_modified = datetime.fromtimestamp(utime)
        get_file_from_sftp(sftp, fileName, last_modified, localDownload_dir, rerunSurveyDate, paperSurveyExtension, imageDownloadDir, imageExtension, loadFileWithoutImages)

    # Close the connection
    sftp.close()
    return fileList
    
def delete_files_from_sftp(pFileList, pArchiveDirectory):
    global imageFileList
    # Get values from the config file
    hostStr = common.read_config_file("sftp","host")
    userName = common.read_config_file("sftp","user_name")
    passWord = common.read_config_file("sftp","password")
    ftp_dir = common.read_config_file("sftp","ftp_dir")
    localDownload_dir = common.read_config_file("sftp","fileDownload_path")
    imageDownloadDir = common.read_config_file("sftp","imageDownload_path")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostStr, username=userName, password=passWord)
    sftp = client.open_sftp()
        
    # Change to the specified FTP directory
    try:
        sftp.chdir(ftp_dir)
    except:
        logging.error('Invalid FTP path for delete: '+ ftp_dir)
        sys.exit(-1)

    for fileName in pFileList:
        # If the survey XML exists in the archive, delete the SFTP version
        if os.path.isfile(pArchiveDirectory + fileName):
            sftp.remove(fileName)

    for fileName in imageFileList:
        # If the survey iamge exists in the archive, delete the SFTP version
        if os.path.isfile(imageDownloadDir + fileName):
            sftp.remove(fileName)
    
                
def check_sftp_counts():
    # Confirm that XML and Image file counts match in Axway
    # A mismatch indicates a problem. Each XML should have one TIF file.
    surveyCount = 0
    imageCount = 0
    # Get values from the config file
    hostStr = common.read_config_file("sftp","host")
    userName = common.read_config_file("sftp","user_name")
    passWord = common.read_config_file("sftp","password")
    ftp_dir = common.read_config_file("sftp","ftp_dir")
    localDownload_dir = common.read_config_file("sftp","fileDownload_path")
    imageDownloadDir = common.read_config_file("sftp","imageDownload_path")
    paperSurveyExtension = common.read_config_file("sftp","file_extension")
    imageExtension = common.read_config_file("sftp","image_extension")
    loadFileWithoutImages = common.read_config_file("sftp","load_without_images")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostStr, username=userName, password=passWord)
    sftp = client.open_sftp()
    for fileName in sftp.listdir(ftp_dir):
        fileFound = re.search(".*\."+ str(paperSurveyExtension) + "$", str(fileName))
        if fileFound:
            surveyCount += 1
        fileFound = re.search(".*\."+ str(imageExtension) + "$", str(fileName))
        if fileFound:
            imageCount += 1

    if surveyCount == 0:
        logging.info('No survey files found: ')
        return(False)
     
    if loadFileWithoutImages != "Yes":
        if surveyCount != imageCount:
            logging.error('XML/Image mismatch. Contact Caso.')
            return(False)
    
    # XML file and Image file counts match. Continue processing
    return(True)
                

    
