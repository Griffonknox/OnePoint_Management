import pandas as pd
from os.path import isfile, join
import pathlib
import pymysql
from sqlalchemy import create_engine, Integer, String
import datetime
import logging
import numpy as np
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


def Query_MemberDB(sql_cnx):
    df = pd.read_sql('SELECT varClientKey FROM memb_account', sql_cnx)  # read the entire table
    return df


def Member_Data_Filter(filename, sql_cnx):
    """CREATE DATAFRAME, FILTER AND FORMAT, RETURN DATAFRAME"""

    #  Read file, skip first row
    # member_upload = pd.read_csv(filename, sep='\s+', skiprows=1, header=None)
    # member_upload = pd.read_csv(filename, header=None)
    member_upload = pd.read_csv(filename, delim_whitespace=True, header=None)  # latest test file

    member_upload = member_upload[[0, 4, 5, 6, 7, 9, 10, 11, 12, 13]]  # pull columns

    member_upload.columns = ["varClientKey", "first_name", "last_name", "phys_address", "mail_address", "phys_city",
                             "phys_state",
                             "phys_zip", "phone", "phone2"]  # rename columns

    # Filter varClientKey column
    # member_upload = member_upload[member_upload["varClientKey"].apply(
    #     lambda x: x.isnumeric())]  # only keep numeric values, removes rows we don't want

    member_upload = member_upload.drop_duplicates(subset=None, keep='first', inplace=False)  # get unique members
    member_upload = member_upload.replace(np.nan, '', regex=True)  # replace nans with empty strings

    # MEMBER MIDDLE NAME, ASK LEO WHAT HE DOES?!?!?!?!
    member_upload["middle_name"] = ""

    # add missing columns for import
    member_upload["detail"] = ""
    member_upload["mail_city"] = ""
    member_upload["mail_state"] = ""
    member_upload["mail_zip"] = ""

    # change column type to integer
    member_upload["varClientKey"] = pd.to_numeric(member_upload["varClientKey"])

    # # query and compare import with DB
    db_members = Query_MemberDB(sql_cnx)["varClientKey"].unique()
    member_upload = member_upload[~member_upload["varClientKey"].isin(db_members)]  # exlude existing members

    return member_upload


def MembData_Sql_Push(dataframe, tablename, sql_cnx):
    """PUSH DATAFRAME TO SQL SERVER """
    #  Push dataframe to sql server

    cnx = sql_cnx
    dtype = {"varClientKey": Integer(), "first_name": String(100), "middle_name": String(100), "last_name": String(100),
             "phys_address": String(50), "phys_city": String(50), "phys_state": String(50), "phys_zip": String(50),
             "mail_address": String(50), "mail_city": String(50), "mail_state": String(50), "mail_zip": String(50),
             "phone": String(20), "phone2": String(20), "detail": String(1000)}
    dataframe.to_sql(tablename, if_exists='append', con=cnx, chunksize=100, index=False, dtype=dtype)

    return True


def Save_New_Members(dataframe, location, save_directory):
    save_location = join(location, save_directory)
    file_name = join(save_location, "{}.csv".format(f'{datetime.datetime.today():%Y-%m-%d}'))
    dataframe.to_csv(file_name, index=False)
    return file_name


def Email_Success(CONFIG, save_url):
    # Create a multipart message
    msg = MIMEMultipart()
    body_part = MIMEText("OnePoint Member Upload Success.\nAttached CSV of this weeks upload.", 'plain')
    msg['Subject'] = "OnePoint Member Upload Success {}".format(f'{datetime.datetime.today():%Y-%m-%d}')
    # Add body to email
    msg.attach(body_part)

    # open and read the CSV file in binary
    with open(save_url, 'rb') as file:
        # Attach the file with filename to the email
        msg.attach(MIMEApplication(file.read(), Name="Upload_{}.csv".format(f'{datetime.datetime.today():%Y-%m-%d}')))

    Email_Send(CONFIG, msg)


def Email_Fail(CONFIG, MSG):
    # Create a multipart message
    msg = MIMEMultipart()
    body_part = MIMEText("OnePoint Member Upload Failed for the following reason\n\n{}.".format(MSG), 'plain')
    msg['Subject'] = "OnePoint Member Upload Failed {}".format(f'{datetime.datetime.today():%Y-%m-%d}')
    # Add body to email
    msg.attach(body_part)

    Email_Send(CONFIG, msg)


def Email_Send(CONFIG, msg):
    server = smtplib.SMTP('smtp.office365.com', 587)
    server.ehlo()
    server.starttls()
    server.login(CONFIG.SMTP_EMAIL, CONFIG.SMTP_PW)

    # Convert the message to a string and send it
    for em in CONFIG.EMAIL_LIST:
        server.sendmail(CONFIG.SMTP_EMAIL, em, msg.as_string())
    server.quit()


def log_file(path, message, filename):
    """LOG EVENTS"""
    current_time = datetime.datetime.now()
    with open(join(path, "Memberlog.txt"), "a") as file:
        file.write("{}, FileName: {}, Date: {}\n".format(message, filename, current_time))

    return True


def Member_Upload_Main(CONFIG):
    try:
        #  Create Upload Dataframe and filter
        upload_df = Member_Data_Filter(join(CONFIG.UPLOAD_LOCATION, CONFIG.UPLOAD_FILES[0]), CONFIG.CNX)
        print(upload_df.head())
        print(upload_df["phys_zip"])

        #  Push Dataframe to SQL Server and create primary key
        table_name = CONFIG.MEMBER_TABLE
        MembData_Sql_Push(upload_df, table_name, CONFIG.CNX)

        # Save Newly Uploaded Members as CSV
        save_url = Save_New_Members(upload_df, CONFIG.STARTING_PATH, CONFIG.SAVE_MEMBERS)

        Log_message = "Member Upload Successful {} New Members Added.".format(len(upload_df.index))
        log_file(CONFIG.STARTING_PATH, Log_message, CONFIG.UPLOAD_FILES[0])

        # Email Success
        Email_Success(CONFIG, save_url)

    except Exception as e:  # Learn how to proper log event
        logging.exception("\n\n{}, MEMBER UPLOAD FAILED FOR THE FOLLOWING FILE {}\n".format(datetime.datetime.now(),
                                                                                            CONFIG.UPLOAD_FILES[0]))
        # Email_Fail(CONFIG, e)
