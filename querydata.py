'''
Deprecated, use fetcher.py instead.

'''



import pandas as pd
import tushare as ts
from tushare.fund import nav
import sys
import os
import datetime
import logging
import utility
import datatypes as dy

today_string = datetime.date.today().strftime("%Y-%m-%d").replace('-', '')
path_name  = '.' + dy.sep + 'originaldata' + dy.sep + today_string + dy.sep


def generate_fund_basic_data(fund_type = 'all', start_day = None, end_day = None):
    # Get all types of fund infortiion.
    # First, save each fund info to a single file.
    # Second, combine the single files to one file.
    fund_type_list = []   
    if fund_type == 'all':
        fund_type_list = ['all', 'equity', 'mix', 'bond', 'monetary']
    else:
        fund_type_list = [fund_type]

    for fund_type in fund_type_list:
        # Generate the fund basic information to local file,
        # save each fund info to a single file.
        # Save each fund file in this folder.
        short_path = path_name + 'fundbasic' + dy.sep + fund_type + 'fund'
        _generate_fund_basic_info(fund_type = fund_type, short_path = short_path, start_day = start_day, end_day = end_day)

        # Combine the single files to one file.
        _combine_fund_basic_data(short_path = short_path)


def _generate_fund_basic_info(fund_type, short_path, start_day = None, end_day = None):
    '''
    Generate the fund basic information to local file,
    Save each fund info to a single file.
    '''
    ongoing = 0
    fund_symbols = get_fund_symbols(fund_type)
    for fund_symbol in fund_symbols:
        ongoing = ongoing + 1
        print('Finished %s/%s' %(ongoing, len(fund_symbols)))
        try:
            fund_basic_info = nav.get_fund_info(fund_symbol)

            fund_info_file = utility.check_path(short_path) + dy.sep + str(fund_symbol) +'.csv'
            print('Saving %s basic infortation to %s ...' %(fund_symbol, fund_info_file))
            fund_basic_info.to_csv(fund_info_file, encoding = 'utf-8')

        except Exception as e:
            print(str(e))
            pass

def _combine_fund_basic_data(short_path):
    '''
    Combine all the funds in the directory together into one file.
    Rename the column names.

    Returns: DataFrame
        symbol    --> symbol             : 基金代码
        jjjc      --> fund_short_name    : 基金简称
        clrq      --> foundation_date    : 成立日期
        ssdd      --> listed_location    : 上市地点
        Type1Name --> operation_mode     : 运作方式
        Type2Name --> fund_category      : 基金类型
        Type3Name --> secondary_category : 二级分类
        jjgm      --> fund_scale         : 基金规模(亿元)
        jjfe      --> fund_amount        : 基金总份额(亿份)
        jjltfe    --> circulation_share  : 上市流通份额(亿份)
        jjferq    --> fund_share_date    : 基金份额日期
        quarter   --> listed_quarter     : 上市季度
        glr       --> fund_company       : 基金管理人
        tgr       --> fund_trustee       : 基金托管人
    '''
    # Read all the fund basic data file and combine them into one file.
    fund_basic_df = pd.DataFrame()
    file_long_names = os.listdir(short_path)

    # Read the first fund to initialize fund_basic_df
    first_file_full_name = short_path + dy.sep + file_long_names[0]
    fund_basic_df = pd.read_csv(first_file_full_name, dtype = {'symbol':str}, encoding = 'utf-8')
    
    for file_long_name in file_long_names[1:]:
        print('Processing fund %s ...' %(file_long_name.split('.')[0]))

        file_full_name = short_path + dy.sep + file_long_name
        per_fund = pd.read_csv(file_full_name, encoding = 'utf-8', dtype = {'symbol' : str})
        # fund_basic_df = pd.merge(fund_basic_df, per_fund, on = 'symbol')
        fund_basic_df = fund_basic_df.append(per_fund)

    del fund_basic_df['jjqc'] # 基金全称
    del fund_basic_df['ssrq'] # 上市日期
    del fund_basic_df['xcr']  # 存续期限

    # Give the column an english name, I think it's better than the old Chinese name.
    fund_basic_df = fund_basic_df.rename(columns = {
                                                    'jjjc'      : 'fund_short_name',
                                                    'clrq'      : 'foundation_date',
                                                    'ssdd'      : 'listed_location',
                                                    'Type1Name' : 'operation_mode',
                                                    'Type2Name' : 'fund_category',
                                                    'Type3Name' : 'secondary_category',
                                                    'jjgm'      : 'fund_scale',
                                                    'jjfe'      : 'fund_amount',
                                                    'jjltfe'    : 'circulation_share',
                                                    'jjferq'    : 'fund_share_date',
                                                    'quarter'   : 'listed_quarter',
                                                    'glr'       : 'fund_company',
                                                    'tgr'       : 'fund_trustee'
                                                   })

    fund_basic_df = fund_basic_df.set_index('symbol')

    for fund_symbol in fund_basic_df.index:
        # Change time format from "%Y-%m-%d %H:%M:%S" to "%Y-%m-%d".
        date = datetime.datetime.strptime(fund_basic_df.loc[fund_symbol, 'foundation_date'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        fund_basic_df.loc[fund_symbol, 'foundation_date'] = date
        
        date = datetime.datetime.strptime(fund_basic_df.loc[fund_symbol, 'fund_share_date'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        fund_basic_df.loc[fund_symbol, 'fund_share_date'] = date

        # Remain 3 decimals
        fund_basic_df.loc[fund_symbol, 'fund_amount'] = round(fund_basic_df.loc[fund_symbol, 'fund_amount'], 3)
        fund_basic_df.loc[fund_symbol, 'circulation_share'] = round(fund_basic_df.loc[fund_symbol, 'circulation_share'], 3)

    fund_info_file = utility.check_path(short_path) + '_basic' +'.csv'
    print('Saving fund basic infortation to %s ...' %(fund_info_file))
    fund_basic_df.to_csv(fund_info_file, encoding = 'utf-8')


def generate_fund_history_data(fund_type, start_day = None, end_day = None):
    '''
    Get the fund history information and save it to a csv file per fund.

    Inputs:
        fund_type - one of ['all', 'equity', 'mix', 'bond', 'monetary']
        start_day - '2011-09-11' if not set.
        end_day   - today if not set.
    '''

    if start_day is None:
        start_day = '2011-09-11'
    if end_day is None:
        end_day = datetime.date.today().strftime("%Y-%m-%d")

    if fund_type == 'all':
        fund_type_list = ['all', 'equity', 'mix', 'bond', 'monetary']
    else:
        fund_type_list = [fund_type]

    for fund_type in fund_type_list:
        fund_symbols = get_fund_symbols(fund_type)
        for fund_symbol in fund_symbols[:]:
            try:
                # print('Getting %s history infortation...' %(fund_symbol))
                fund_his_data = nav.get_nav_history(fund_symbol,
                                   start = start_day,
                                   end = end_day,
                                   retry_count = 5,
                                   timeout = 20)
        
                fund_info_file = utility.check_path(path_name + 'fundhistory' + dy.sep + fund_type + 'fund') + dy.sep + fund_symbol +'.csv'
                print('Saving %s history infortation to %s ...' %(fund_symbol, fund_info_file))
                fund_his_data.to_csv(fund_info_file)
            except Exception as e:
                print(e)
                pass


def generate_fund_expand_data(fund_type):
    '''
    Not used now.
    '''
    fund_symbols = get_fund_symbols(fund_type)
    fund_info_df = pd.DataFrame()
    fund_info = pd.DataFrame()
    fund_info_list = []

    is_first = True

    try:
        for fund_symbol in fund_symbols[:5]:
            fund_info = nav.get_fund_info(fund_symbol)
            #fund_info['symbol'] = fund_symbol
            fund_info_list.append(fund_info.iloc[0].values)

            #fund_info.set_index('symbol')
            #print(fund_info.values)

            """
            if is_first:
                is_first = False

                print('Getting %s information...' %(fund_symbol))
                fund_info_df = nav.get_fund_info(fund_symbol)
                print(fund_info_df)
            else:             
                print('Getting %s information...' %(fund_symbol))
                fund_info = nav.get_fund_info(fund_symbol)
                fund_info_df.append(fund_info.iloc[0])
                print(fund_info_df)
                """
    except Exception as e:
        print(e)
        pass

    fund_info_file = path_name + 'original' + dy.sep + 'OpenFund_' + fund_type + '_expand' +'.csv'
    print('Saving fund expand information to %s...' %(fund_info_file))
    fund_info_df = pd.DataFrame(fund_info_list, columns = fund_info.columns)
    fund_info_df.to_csv(fund_info_file)

    return fund_info_df


def generate_shanghai_index_data(start_day = None, end_day = None, file_full_path = None):
    '''
    Get the Shanghai Composite Index data and save it to a csv file.

    Inputs:
        start_day - '2011-09-11' if not set.
        end_day   - today if not set.
        file_full_path - The file name that the SCI data will be saved in.
    '''
    if start_day is None:
        start_day = '2011-09-11'
    if end_day is None:
        end_day = datetime.date.today().strftime("%Y-%m-%d")

    if file_full_path is None:
        file_full_path = utility.check_path(path_name + 'general') + dy.sep + 'sh_index.csv'

    print('Getting SCI data...')
    index_data = ts.get_hist_data('sh', start = start_day, end = end_day)

    print('Saving SCI data to %s\n' %(file_full_path))
    index_data.to_csv(file_full_path, encoding = 'utf-8')


def get_fund_symbols(fund_type):
    fund_symbols = []

    try:
        fund_info = nav.get_nav_open(fund_type)
        fund_symbols = fund_info['symbol'].astype(str)
    except Exception as e:
        print(e)
        pass

    return fund_symbols


def get_today_string():
    '''
    If today is 2016-10-01, returns 20161001
    '''
    today_string = datetime.date.today().strftime("%Y-%m-%d").replace('-', '')

    return today


