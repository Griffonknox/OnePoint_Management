from .config import ScriptConfig
from sqlalchemy import create_engine
import pandas as pd
from datetime import timedelta, datetime
import pathlib
from os.path import join
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import smtplib
import logging

def sql_cnx():
    username = ScriptConfig.USERNAME
    password = ScriptConfig.PASSWORD
    host = ScriptConfig.HOST
    dbname = ScriptConfig.DBNAME

    url = "mysql+pymysql://{}:{}@{}/{}".format(username, password, host, dbname)
    cnx = create_engine(url)
    return cnx

"""QUERY FOLLOWUPS AND RETURN DATAFRAME"""
def queryFollow_Ups():

    """QUERY FOLLOWUPS FROM DB"""
    # df = pd.read_sql('SELECT * FROM follow_up', sql_cnx())  # read the entire table

    """TESTING JUST PULL DATA FROM CSV"""
    df = pd.read_csv("testing.csv")

    return df

def filterFollow_Ups(df):

    df['dateEntered'] = pd.to_datetime(df['dateEntered'])


    """GET DATE TO FILTER FROM SUNDAY TO LAST SUNDAY"""
    now = datetime.today()
    sunday = now - timedelta(days=now.weekday()) - timedelta(days=1)
    lst_sunday = sunday - timedelta(weeks=1)
    print(sunday)
    print(lst_sunday)

    """FILTER DATE"""
    df_last_week = df[(df['dateEntered'] >= lst_sunday) & (df['dateEntered'] <= sunday)]

    return df_last_week


def countFollow_Ups(df):

    users = df["varEnteredBy"].unique()
    results = "\n\nUsername: FollowUps Created"
    for user in users:
        sort = df[df["varEnteredBy"] == user]
        results += "\n{}: {}".format(user, len(sort.index))

    return results


def saveFollow_Ups(dataframe):

    starting_path = pathlib.Path(__file__).parent.absolute()
    save_location = join(starting_path, ScriptConfig.SAVE_LOCATION)
    file_name = join(save_location, "FollowUp_Report_{}.csv".format(f'{datetime.today():%Y-%m-%d}'))
    dataframe.to_csv(file_name, index=False)
    return file_name

def Email_Success(save_url, results):
    # Create a multipart message
    msg = MIMEMultipart()
    body_part = MIMEText("Weekly OnePoint FollowUp Report for the previous week.\nFull report is attached, quick statistics displayed below.{}".format(results), 'plain')
    msg['Subject'] = "Weekly OnePoint FollowUp Report {}".format(f'{datetime.today():%Y-%m-%d}')
    # Add body to email
    msg.attach(body_part)

    # open and read the CSV file in binary
    with open(save_url, 'rb') as file:
        # Attach the file with filename to the email
        msg.attach(MIMEApplication(file.read(), Name="FollowUp_Report_{}.csv".format(f'{datetime.today():%Y-%m-%d}')))

    Email_Send(ScriptConfig, msg)

def Email_Send(CONFIG, msg):
    server = smtplib.SMTP('smtp.office365.com', 587)
    server.ehlo()
    server.starttls()
    server.login(CONFIG.SMTP_EMAIL, CONFIG.SMTP_PW)

    # Convert the message to a string and send it
    for em in CONFIG.EMAIL_LIST:
        server.sendmail(CONFIG.SMTP_EMAIL, em, msg.as_string())
    server.quit()

def Email_Fail(MSG):
    # Create a multipart message
    msg = MIMEMultipart()
    body_part = MIMEText("OnePoint Follow Up Report Failed  for the following reason\n\n{}.".format(MSG), 'plain')
    msg['Subject'] = "OnePoint Follow Up Report Failed {}".format(f'{datetime.today():%Y-%m-%d}')
    # Add body to email
    msg.attach(body_part)

    Email_Send(ScriptConfig, msg)


def Main():

    try:

        """QUERY FOLLOWUPS, FILTER FOR PAST WEEK, AND SAVE"""
        df = queryFollow_Ups()
        df = filterFollow_Ups(df)
        save_url = saveFollow_Ups(df)

        """CALCULATE RESULTS, AND EMAIL"""
        results = countFollow_Ups(df)
        Email_Success(save_url, results)

    except Exception as e:
        logging.exception("\n\n {}, Report Failed!".format(datetime.now()))
        Email_Fail( e)