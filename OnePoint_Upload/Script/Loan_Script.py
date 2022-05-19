import pandas as pd
from os.path import isfile, join
import pathlib
import pymysql
from sqlalchemy import create_engine, Integer, String
import datetime
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def Loan_Data_Filter(filename, sharename):
    """CREATE DATAFRAME, FILTER AND FORMAT, RETURN DATAFRAME"""

    #  Read file, skip first row
    loan_upload = pd.read_csv(filename, delim_whitespace=True, header=None)  # latest test file

    loan_upload = loan_upload[[0, 1, 2, 3]]  # pull first 4 columns

    loan_upload.columns = ["varClientKey", "loan_numb", "acctnolnno", "balance"]  # rename columns

    #  Filter varClientKey column
    # loan_upload = loan_upload[loan_upload["varClientKey"].apply(
    #     lambda x: x.isnumeric())]  # only keep numeric values, removes rows we don't want

    loan_upload["loan_numb"] = loan_upload["loan_numb"].astype(str)
    loan_upload["loan_numb"] = loan_upload["loan_numb"].str.zfill(2)  # make number 2 digits

    loan_upload["varClientKey"] = loan_upload["varClientKey"].astype(str)
    loan_upload["acctnolnno"] = loan_upload["varClientKey"] + loan_upload[
        "loan_numb"]  # get correct format of acctnolnno


    # shared loans
    share_upload = pd.read_csv(sharename, delim_whitespace=True, header=None)  # latest test file
    share_upload = share_upload[[0, 1, 2]]  # pull first 3 columns
    share_upload.columns = ["varClientKey", "loan_numb", "balance"]



    share_upload["loan_numb"] = share_upload["loan_numb"].astype(str)
    share_upload["loan_numb"] = share_upload["loan_numb"].str.zfill(2)  # make number 2 digits

    share_upload["varClientKey"] = share_upload["varClientKey"].astype(str)
    share_upload["acctnolnno"] = share_upload["varClientKey"] + share_upload[
        "loan_numb"]  # get correct format of acctnolnno


    # CONCATENATE DATAFRAMES
    total_loans = pd.concat([loan_upload, share_upload])


    # additional columns we need for upload
    total_loans["varEnteredBy"] = ""
    total_loans["dateEntered"] = ""
    total_loans["key"] = range(1, len(total_loans) + 1)

    return total_loans


def LoanData_Sql_Push(dataframe, tablename, sql_cnx):
    """PUSH DATAFRAME TO SQL SERVER AND CREATE PRIMARY KEY"""
    #  Push dataframe to sql server and create primary key

    cnx = sql_cnx

    with cnx.connect() as con:
        text = con.execute("""TRUNCATE TABLE acct_loans""")

    # FOREIGN KEY CONSTRAINT WITH MEMBERS
    db_members = pd.read_sql('SELECT varClientKey FROM memb_account', sql_cnx)
    print(db_members)
    db_members = db_members["varClientKey"].astype(str).unique()
    boolean_series = dataframe["varClientKey"].isin(db_members) # select only followups that have accounts to them
    filtered_df = dataframe[boolean_series]

    print("filterpushtodb")
    print(filtered_df)

    filtered_df.to_sql(tablename, if_exists='append', con=cnx, chunksize=100, index=False)

    # dtype = {"key": Integer(), "varClientKey": Integer(), "dateEntered": String(50), "varEnteredBy": String(100),
    #          "loan_numb": String(25), "acctnolnno": Integer(), "balance": String(20)}
    # dataframe.to_sql(tablename, if_exists='replace', con=cnx, chunksize=100, index=False, dtype=dtype)

    return True




def Email_Success(CONFIG):

    # Create a multipart message
    msg = MIMEMultipart()
    body_part = MIMEText("OnePoint Loan Upload Success.", 'plain')
    msg['Subject'] = "OnePoint Loan Upload Success {}".format(f'{datetime.datetime.today():%Y-%m-%d}')
    # Add body to email
    msg.attach(body_part)

    Email_Send(CONFIG, msg)


def Email_Fail(CONFIG, MSG):
    # Create a multipart message
    msg = MIMEMultipart()
    body_part = MIMEText("OnePoint Loan Upload Failed for the following reason\n\n{}.".format(MSG), 'plain')
    msg['Subject'] = "OnePoint Loan Upload Failed {}".format(f'{datetime.datetime.today():%Y-%m-%d}')
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


def log_file(path, message, filename):
    """LOG EVENTS"""
    current_time = datetime.datetime.now()
    with open(join(path, 'Loanlog.txt'), "a") as file:
        file.write("{}, FileName: {}, Date: {}\n".format(message, filename, current_time))

    return True


def Loan_Upload_Main(CONFIG):

    try:

        #  Create Upload Dataframe and filter
        upload_df = Loan_Data_Filter(join(CONFIG.UPLOAD_LOCATION, CONFIG.UPLOAD_FILES[0]), join(CONFIG.UPLOAD_LOCATION, CONFIG.UPLOAD_FILES[1]))
        print(upload_df)

        #  Push Dataframe to SQL Server and create primary key
        table_name = CONFIG.LOAN_TABLE
        LoanData_Sql_Push(upload_df, table_name, CONFIG.CNX)

        log_file(CONFIG.STARTING_PATH, "Loan Upload successful:", CONFIG.UPLOAD_FILES[0])

        Email_Success(CONFIG)

    except Exception as e:  # Learn how to proper log event
        logging.exception("LOAN UPLOAD FAILED FOR THE FOLLOWING FILE {}".format(CONFIG.UPLOAD_FILES[0]))
        # Email_Fail(CONFIG, e)
