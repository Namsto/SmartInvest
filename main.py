import pandas as pd
import querydata
import tushare as ts
import fetcher
import datatypes as dt
import datetime
import utility
#import featureengineering as fe

today_string = datetime.date.today().strftime("%Y-%m-%d").replace('-', '')
ORIGINAL_DATA_PATH  = '.' + dt.sep + 'data' + dt.sep + 'originaldata' + dt.sep + today_string + dt.sep
ORIGINAL_FUND_DATA_PATH = ORIGINAL_DATA_PATH + 'fund' + dt.sep 



if __name__ == "__main__":
    # Generate all types of funds information to local csv file.
    # First, save each fund info to a single file.
    # Second, combine the single files to one file,
    # For example, save to file .\originaldata\20161116\fundbasic\mixfund_basic.csv.
    # querydata.generate_fund_basic_data('mix')

    # Generate all funds history information to local csv file.
    # Like the net value, change every day.
    # Each fund will be saved to one csv file,
    # for example, save to file .\originaldata\20161116\fundhistory\mixfund\000363.csv
    # querydata.generate_fund_history_data('mix')

    # Get index information and saving to local file.
    # querydata.generate_shanghai_index_data(end_day = '2016-10-22')
    # querydata.generate_shanghai_index_data()

    ###########################################
    # Feature engineering
    # Run in featureengineering.py main() function.
    # fe.process_fund_daily_data(is_accuracy = True)



    fund_types = ['mix']
    path = utility.check_path(ORIGINAL_FUND_DATA_PATH) + "mix_fund_basic.csv"
    history_info_path = utility.check_path(ORIGINAL_FUND_DATA_PATH + 'historyinfo' + dt.sep)
    sh_index_file = utility.check_path(ORIGINAL_FUND_DATA_PATH) + "sh_index.csv"

    fc = fetcher.Fecther()
    fc.fetch_fund_basic_data(fund_types, path)
    fc.fetch_fund_history_data(fund_types, path=history_info_path)
    fc.fetch_shanghai_index(sh_index_file)


