from os import listdir
from os.path import isfile, join
import pathlib
import datetime
from sqlalchemy import create_engine
import shutil
import logging
from Script.Member_Script import Member_Upload_Main
from Script.Loan_Script import Loan_Upload_Main
from Script.config import ScriptConfig
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

logging.basicConfig(filename="Error_Log.txt", filemode='a', level=logging.DEBUG)



def sql_cnx():
    username = ScriptConfig.USERNAME
    password = ScriptConfig.PASSWORD
    host = ScriptConfig.HOST
    dbname = ScriptConfig.DBNAME

    url = "mysql+pymysql://{}:{}@{}/{}".format(username, password, host, dbname)
    cnx = create_engine(url)
    return cnx

def Data_Completion(upload_path, finish_path, filename):
    """MOVE DATA FILE TO COMPLETE DIRECTORY"""
    shutil.move(join(upload_path, filename), join(finish_path,"{}{}{}".format(f'{datetime.datetime.today():%Y-%m-%d}', "_completed_", filename)))
    return True



def Read_Directory(path):
    """RETURN LIST OF ALL FILES IN DIRECTORY"""
    onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]
    return onlyfiles


def Email_Send(CONFIG, MSG):

    msg = MIMEMultipart()
    body_part = MIMEText("OnePoint Upload Script Failed for the following reason.\n\n{}.".format(MSG), 'plain')
    msg['Subject'] = "OnePoint Upload Script Failed. {}".format(f'{datetime.datetime.today():%Y-%m-%d}')
    # Add body to email
    msg.attach(body_part)

    server = smtplib.SMTP('smtp.office365.com',587)
    server.ehlo()
    server.starttls()
    server.login(CONFIG.SMTP_EMAIL, CONFIG.SMTP_PW)

    # Convert the message to a string and send it
    for em in CONFIG.EMAIL_LIST:
        server.sendmail(CONFIG.SMTP_EMAIL, em, msg.as_string())
    server.quit()


def Main():

    try:
        # Get starting path name
        ScriptConfig.STARTING_PATH = pathlib.Path(__file__).parent.absolute()

        upload_path = join(ScriptConfig.STARTING_PATH, ScriptConfig.UPLOAD_LOCATION)  # edit to path start, where data is store for upload

        # Connect to DB
        ScriptConfig.CNX = sql_cnx()

        #  Get Files in Directory
        ScriptConfig.UPLOAD_FILES = Read_Directory(upload_path)
        print(ScriptConfig.UPLOAD_FILES)


        # The Full Member Upload Function
        # Member_Upload_Main(ScriptConfig)

        # The Full Loan Upload Function
        Loan_Upload_Main(ScriptConfig)


        #  Move File from upload to finish
        # finish_path = join(ScriptConfig.STARTING_PATH, ScriptConfig.COMPLETE_LOCATION)  # Edit to path destination
        # Data_Completion(upload_path, finish_path, ScriptConfig.UPLOAD_FILES[0])

    except Exception as e:  # Learn how to proper log event
        logging.exception("\n\n {}, UPLOAD FAILED!".format(datetime.datetime.now()))
        # Email_Send(ScriptConfig, e)


if __name__ == "__main__":
    Main()
