import pandas as pd
import numpy as np
import tushare as ts
from tushare.fund import nav
import sys
import os
import datetime
import logging
from tqdm import tqdm

import utility
import datatypes as dt


class Fecther():
    """
    Fecth the fund information from website.
    """

    def fetch_fund_basic_data(self, fund_types, path):
        """
        Fecth the fund basic information.


        Parameters
        ----------

        fund_types : list
            The type of fund.
            For example:
                ['all']
                ['all', 'equity', 'mix', 'bond', 'monetary']


        path : string
            The file full spec to save the data.


        Returns
        -------
        info_df : DataFrame

            Rename
                symbol    --> symbol             : 基金代码
                jjqc      --> fund_full_name     : 基金全称
                jjjc      --> fund_short_name    : 基金简称
                clrq      --> foundation_date    : 成立日期
                ssrq      --> listing_date       : 上市日期
                xcr       --> renew_period       : 续存期限
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
        """
        
        fund_symbols = list()
        for fund_type in fund_types:
            fund_symbols += self._get_fund_symbols(fund_type)

        # Remove duplicated symbols
        fund_symbols = list(set(fund_symbols))


        if fund_symbols is None or len(fund_symbols) <= 0:
            print("ERROR!!! fund_symbols is None or len(fund_symbols) <= 0")

            return None

        info_df = nav.get_fund_info(fund_symbols[0])

        try:
            print("Getting fund basic information...")


            failed_symbols = list()

            for fund_symbol in tqdm(fund_symbols[1:]):
                try:
                    info_df = info_df.append(nav.get_fund_info(fund_symbol))
                except Exception as e:
                    failed_symbols.append(fund_symbol)
                    print("fund_symbol={}".format(fund_symbol))
                    print(e)
                    pass

            # Try to get the failed fund again.
            failed_symbols2 = list()
            if len(failed_symbols) > 0:
                for symbol in failed_symbols:
                    try:
                        info_df = info_df.append(nav.get_fund_info(symbol))
                    except Exception as e:
                        failed_symbols2.append(symbol)
                        print(e)
                        pass

            if len(failed_symbols2) > 0:
                print("Still failed to get {} funds information.".format(len(failed_symbols2)))
                print(failed_symbols2)


            # Give the column an english name, I think it's better than the old Chinese name.
            info_df = info_df.rename(columns = {
                                                'jjqc'      : 'fund_full_name',
                                                'jjjc'      : 'fund_short_name',
                                                'clrq'      : 'foundation_date',
                                                'ssrq'      : 'listing_date',
                                                'xcr'       : 'renew_period',
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
                                                'tgr'       : 'fund_trustee'})


            info_df = info_df.sort_index()

            # TODO: think out a more efficient way
            for symbol in info_df.index:
                # Change time format from "%Y-%m-%d %H:%M:%S" to "%Y-%m-%d".
                date = datetime.datetime.strptime(info_df.loc[symbol, 'foundation_date'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
                info_df.loc[symbol, 'foundation_date'] = date
        
                date = datetime.datetime.strptime(info_df.loc[symbol, 'fund_share_date'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
                info_df.loc[symbol, 'fund_share_date'] = date

                # Remain 3 decimals
                # need to convert to float first
                #info_df.loc[symbol, 'fund_amount'] = round(info_df.loc[symbol, 'fund_amount'], 3)
                #info_df.loc[symbol, 'circulation_share'] = round(info_df.loc[symbol, 'circulation_share'], 3)


            info_df.to_csv(path, encoding='utf-8')
            print("Savd {} basic information to {}".format(len(fund_symbols), path))

        except Exception as e:
            print("ERROR happened in fetch_fund_basic_data")
            print(str(e))
            pass

        return info_df


    def fetch_fund_history_data(self, fund_types, path, start_day = '2011-09-11', end_day = None):
        '''
        Get the fund history information and save it to a csv file per fund.


        Parameters
        ----------
        fund_types : list of string
            the fund types
            For example:
                ['all']
                ['all', 'equity', 'mix', 'bond', 'monetary']
                ...

        path : string
            the path to save the funds files

        start_day : string, default is '2011-09-11'
            the start day of the fund information


        end_day : string, default is today
            end day for fund history

        Returns
        -------
        None

        '''

        if end_day is None:
            end_day = datetime.date.today().strftime("%Y-%m-%d")


        fund_symbols = list()
        for fund_type in fund_types:
            fund_symbols += self._get_fund_symbols(fund_type)


        # Remove duplicated symbols
        fund_symbols = list(set(fund_symbols))

        for fund_symbol in tqdm(fund_symbols[:]):
            try:
                # print('Getting %s history infortation...' %(fund_symbol))
                his_df = nav.get_nav_history(fund_symbol,
                                   start = start_day,
                                   end = end_day,
                                   retry_count = 5,
                                   timeout = 20)
        
                file_spec = path + fund_symbol +'.csv'
                his_df.to_csv(file_spec)
                print('\n Saved %s' %(file_spec))
            except Exception as e:
                print(e)
                pass


    def fetch_shanghai_index(self, filename, start_day = '2011-09-11' , end_day = None):
        '''
        Get the Shanghai Composite Index data and save it to a csv file.

        Parameters
        ----------
            filename : string
                filename to save the SCI data

            start_day : string, default is '2011-09-11'

            end_day : string, default is today
        '''

        if end_day is None:
            end_day = datetime.date.today().strftime("%Y-%m-%d")

        index_data = ts.get_hist_data('sh', start = start_day, end = end_day)

        index_data.to_csv(filename, encoding = 'utf-8')
        print('Saved Shanghai Composite Index data to %s\n' %(filename))


    def _get_fund_symbols(self, fund_type):
        """
        Get the fund symbols of the specific fund type.


        Parameters
        ----------
        fund_type : string
            The fund type, one of ['all', 'equity', 'mix', 'bond', 'monetary'].



        Returns:
        --------
        fund_symbols : list
            A list of fund symbols.

        """

        fund_symbols = []

        try:
            fund_info = nav.get_nav_open(fund_type)
            fund_symbols = fund_info['symbol'].astype(str)
        except Exception as e:
            print(e)
            pass

        return list(fund_symbols)


