from .config import ScriptConfig
from sqlalchemy import create_engine
import pandas as pd
from datetime import date, timedelta, datetime
import pathlib
from os.path import join

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
    print(df_last_week.info())

    return df_last_week


def saveFollow_Ups(dataframe):

    starting_path = pathlib.Path(__file__).parent.absolute()
    save_location = join(starting_path, ScriptConfig.SAVE_LOCATION)
    file_name = join(save_location, "{}.csv".format(f'{datetime.today():%Y-%m-%d}'))
    dataframe.to_csv(file_name, index=False)
    return file_name