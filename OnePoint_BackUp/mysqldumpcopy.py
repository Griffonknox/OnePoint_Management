import os
import datetime
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from config import ScriptConfig
import logging
from os.path import join
import shutil
import time

logging.basicConfig(filename=ScriptConfig.ERROR_LOG, filemode='a', level=logging.DEBUG)


def mysqldumpcommand():
    cmd_string = "mysqldump -u{} -p{} onepoint > C:\inetpub\wwwroot\OnePoint_BackUp\Data\onepointbackup_{}.sql".format(ScriptConfig.DB_USR, ScriptConfig.DB_PW, f'{datetime.datetime.today():%Y_%m_%d}')

    os.system(cmd_string)


def Email_Success(CONFIG):

    # Create a multipart message
    msg = MIMEMultipart()
    body_part = MIMEText("OnePoint Database Back Up Success.", 'plain')
    msg['Subject'] = "OnePoint Database Back Up Success.".format(f'{datetime.datetime.today():%Y-%m-%d}')
    # Add body to email
    msg.attach(body_part)

    Email_Send(CONFIG, msg)


def Email_Fail(CONFIG, MSG):
    # Create a multipart message
    msg = MIMEMultipart()
    body_part = MIMEText("OnePoint Database Back Up Failed for the following reason\n\n{}.".format(MSG), 'plain')
    msg['Subject'] = "OnePoint Database Back Up Failed".format(f'{datetime.datetime.today():%Y-%m-%d}')
    # Add body to email
    msg.attach(body_part)

    Email_Send(CONFIG, msg)

def Email_Send(CONFIG, msg):
    server = smtplib.SMTP('smtp.office365.com',587)
    server.ehlo()
    server.starttls()
    server.login(CONFIG.SMTP_EMAIL, CONFIG.SMTP_PW)

    # Convert the message to a string and send it
    for em in CONFIG.EMAIL_LIST:
        server.sendmail(CONFIG.SMTP_EMAIL, em, msg.as_string())
    server.quit()

def Data_Completion(upload_path, finish_path, filename):
    """MOVE DATA FILE TO COMPLETE DIRECTORY"""
    shutil.copy(join(upload_path, filename), join(finish_path,file_name))
    return True


if __name__ == '__main__':

    try:

        file_name = "onepointbackup_{}.sql".format(f'{datetime.datetime.today():%Y_%m_%d}')
        Data_Completion("C:\inetpub\wwwroot\OnePoint_BackUp\Data", r"O:\Data", file_name)

    except Exception as e:  # Learn how to proper log event
        logging.exception("DATABASE COPY FAILED!!")
        #Email_Fail(ScriptConfig, e)
