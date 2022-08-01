from .config import ScriptConfig
from sqlalchemy import create_engine
import pandas as pd
from datetime import date, timedelta, datetime

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


    """GET DATE TO FILTER BY MONDAY"""
    today = datetime.today()
    week_prior = today - timedelta(weeks=1)
    yesterday = today - timedelta(days=1)


    df_last_week = df[(df['dateEntered'] >= week_prior)]

    print(df_last_week.info())

    df_last_week.to_csv("testing_filter.csv")
