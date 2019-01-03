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


imageFileList = []


def archive_file(pFileName, pLocalDir, pArchiveDir):
    # Moves files from the working directoy to Archive
    # Both directories are defined in config.json
    try:
        shutil.copy2(pLocalDir + pFileName, pArchiveDir)
        if path.exists(pArchiveDir + pFileName):
            try:
                os.remove(pLocalDir + pFileName)
            except  Exception as e:
                logging.error('Error deleting: ' + pLocalDir + pFileName + str(e))
    except Exception as e:
        logging.error('archiveFile error: ' + str(e))
    
    
def get_file_from_sftp(fileList, pSrv, pFileName, pLastModified, pFileDownloadDir, pLatestSurveyDate, pPaperSurveyExtension, pImageDownloadDir, pImageExtension, pLoadWithoutImages):
    global imageFileList

    
    try:    
        archive_dir = common.read_config_file("sftp","archive_dir")
        file_name = pFileName
        foundAtLeastOneXML = 'N'
        
        if 1==1: ## pLastModified >= pLatestSurveyDate:
            #Only pull XML files with right extention
        
            xmlFileFound = re.search(".*\."+ pPaperSurveyExtension + "$", file_name)
            if xmlFileFound:
                foundAtLeastOneXML = 'Y'
                if os.path.isfile(pFileDownloadDir + file_name):
                    logging.error('File ' + file_name +' already processed.')
                else:
                    fileList.append(file_name)
                    pSrv.get(file_name, pFileDownloadDir + file_name)
                    logging.info('Downloaded XML: ' + file_name)
        return fileList

    except ValueError:
        logging.error('getFileFromFTP invalid date: ' + str(pFtpObject.st_mtime))

def moveImagesFromSFTP(pSftp, pMaxNumberOfFiles):
    global imageFileList

    try:
        imageExtension = common.read_config_file("sftp","image_extension")
        imageDownloadDir = common.read_config_file("sftp","imageDownload_path")
        ftp_dir = common.read_config_file("sftp","ftp_dir")

        # Get values from the config file
        hostStr = common.read_config_file("sftp","host")
        userName = common.read_config_file("sftp","user_name")
        passWord = common.read_config_file("sftp","password")
        MaxNumberOfFiles = common.read_config_file("sftp","max_number_of_files")
    
        fileFound = 'Y'
        while fileFound == 'Y':
            # List the files
            fileCount = 0
            fileFound = 'N'
            for fileName in pSftp.listdir(ftp_dir):
                if fileCount > pMaxNumberOfFiles:
                    break
        
                #Only pull Tif files with right extention
                imageFileFound = re.search(".*\."+ imageExtension + "$", fileName)
                if imageFileFound:
                    fileFound = 'Y'
                    #Process each file, get modified date
##                    utime = sftp.stat(pFileName).st_mtime
##                    last_modified = datetime.fromtimestamp(utime)
                    if os.path.isfile(imageDownloadDir + fileName):
                        logging.error('Image ' + fileName +' already processed.')
                    else:
                        imageFileList.append(fileName)
                        pSftp.get(fileName, imageDownloadDir + fileName)
                        logging.info('Downloaded image: ' + fileName)

                # If the survey image exists in the archive, delete the SFTP version
                if os.path.isfile(imageDownloadDir + fileName):
                    try:
                        pSftp.remove(fileName)
                    except:
                        logging.error('Delete from FTP error: ' + fileName)
                if fileCount > MaxNumberOfFiles:
                    break
                    
    except ValueError:
        logging.error('getImageFileFromFTP invalid date: ' + str(pFtpObject.st_mtime))
    

def get_files(fileList, pSftp, pCursor, pMaxNumberOfFiles):

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


    # Change to the specified FTP directory
    try:
        pSftp.chdir(ftp_dir)
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

    
    # List the files
    fileCount = 0
    for fileName in pSftp.listdir(ftp_dir):
  
        if fileCount > pMaxNumberOfFiles:
            break
        #Process each file, get modified date
        utime = pSftp.stat(fileName).st_mtime
        last_modified = datetime.fromtimestamp(utime)
        fileList = get_file_from_sftp(fileList, pSftp, fileName, last_modified, localDownload_dir, rerunSurveyDate, paperSurveyExtension, imageDownloadDir, imageExtension, loadFileWithoutImages)
        fileCount += 1

    return fileList
    
def delete_files_from_sftp(pSftp, pFileName, pArchiveDirectory):
    global imageFileList
    # Get values from the config file
    ftp_dir = common.read_config_file("sftp","ftp_dir")
    localDownload_dir = common.read_config_file("sftp","fileDownload_path")
    imageDownloadDir = common.read_config_file("sftp","imageDownload_path")

        
    # Change to the specified FTP directory
    try:
        pSftp.chdir(ftp_dir)
    except:
        logging.error('Invalid FTP path for delete: '+ ftp_dir)
        sys.exit(-1)
        
    # If the survey XML exists in the archive, delete the SFTP version
    try:
        if os.path.isfile(pArchiveDirectory + pFileName):
            try:
                pSftp.remove(pFileName)
            except Exception as e:
                print(e.args)
                logging.error('Cannot delete: '+ pFileName + ' from SFTP site')
    except:
        logging.error('Cannot find: '+ pFileName + ' in ' + pArchiveDirectory)
        
    
                
def check_sftp_counts(pSftp):
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


    for fileName in pSftp.listdir(ftp_dir):
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
                

    
