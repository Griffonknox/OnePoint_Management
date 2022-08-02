from OnePoint_Followup_Report.utils import queryFollow_Ups, filterFollow_Ups, saveFollow_Ups
import logging

logging.basicConfig(filename="Error_Log.txt", filemode='a', level=logging.DEBUG)


def Main():
    # try:

    df = queryFollow_Ups()
    df = filterFollow_Ups(df)
    save_url = saveFollow_Ups(df)
    print(save_url)
    print("finishedasdfasdfasdf")


    # except:
    #     # logging.exception("\n\n {}, Report Failed!".format(datetime.now()))
    #     print("failed")

if __name__ == "__main__":
    Main()
