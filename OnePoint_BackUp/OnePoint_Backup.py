from sqlalchemy import create_engine
import pandas as pd


def sql_cnx():
    username = USERNAME
    password = PASSWORD
    host = HOST
    dbname = DBNAME

    url = "mysql+pymysql://{}:{}@{}/{}".format(username, password, host, dbname)
    cnx = create_engine(url)
    return cnx


def Query_DB(name, sql_cnx):
    df = pd.read_sql('SELECT * FROM {}'.format(name), sql_cnx)  # read the entire table
    return df



if __name__ == '__main__':

    cnx = sql_cnx()

    # member_db = Query_DB("memb_account", cnx)
    # member_db.to_csv("Member_Account.csv", index=False)

    loans_db = Query_DB("acct_loans", cnx)
    loans_db.to_csv("Loans.csv", index=False)

    alert_db = Query_DB("alert", cnx)
    alert_db.to_csv("Alerts.csv", index=False)

    follow_db = Query_DB("follow_up", cnx)
    follow_db.to_csv("Follow_Ups.csv", index=False)

    user_db = Query_DB("usr", cnx)
    user_db.to_csv("Users.csv", index=False)

