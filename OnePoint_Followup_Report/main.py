from OnePoint_Followup_Report.utils import queryFollow_Ups, filterFollow_Ups
import logging
from datetime import datetime, timedelta, date

logging.basicConfig(filename="Error_Log.txt", filemode='a', level=logging.DEBUG)


def Main():
    # try:

    df = queryFollow_Ups()
    filterFollow_Ups(df)
    print("finished")

    # except:
    #     # logging.exception("\n\n {}, Report Failed!".format(datetime.now()))
    #     print("failed")

if __name__ == "__main__":
    Main()
