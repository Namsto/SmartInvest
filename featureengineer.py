import numpy as np
import pandas as pd
import sklearn
import matplotlib.pyplot as plt
import os
import datatypes as dy



def process_fund_history_data(is_analysis_label, output_file_long_path, is_accuracy = True):
    '''
    Construct some features from the fund daily data.
    Saves a DataFrame to a local file and returns this DataFrame.

    Inputs:
        is_analysis_label - True: use fund full history data to analysis and label the funds.
                            False: use per_fund_df[dy.[MONTH]:dy.TOTAL_DAYS] to build the model.

        is_accuracy - True: means that the days we use to analysis in SCI and per fund must be the same.
                    - False: means that the days do not need to be the same, but the number of days need to be equal.
                    Default is True


    Returns: DataFrame
        symbol  - fund symbol
        days_of_processed - days 
        rate_of_rise_days - The percentage that this fund rise.
        rate_of_higher_than_sci - The percentage that this fund rise higher than Shanghai Composite Index.
        rate_of_rise_and_higher_than_sci - The percentage that this fund rise and higher than SCI.
    '''
    # Process the fund daily data.
    file_short_path = r'.\data\processingdata\fund\historyinfo'
    file_long_names = os.listdir(file_short_path)
    print('Processing %s funds...' %(len(file_long_names)))

    # Read Shanghai Composite Index data.
    sci_df_original = pd.read_csv(r'.\data\processingdata\fund\sh_index.csv', index_col = 'date')

    skipped_funds = []
    fund_df = pd.DataFrame()
    fund_change_ave = []
    fund_symbols_list = []
    days_of_processed_list = []
    rate_of_rise_days_list = []
    rate_of_higher_than_sci_list = []
    rate_of_rise_and_higher_than_sci_list = []

    # file_name is 000001.csv
    for file_name in file_long_names[:]:
        fund_symbol = file_name.split('.')[0]
        print('Processing fund %s ...' %(fund_symbol))

        file_full_name = file_short_path + dy.sep + file_name
        per_fund_df = pd.read_csv(file_full_name, index_col = 'date')

        # Define how many days we will use to build our model.
        start_day = 0
        end_day = dy.TOTAL_DAYS

        # Some fund is newly set up and len(per_fund_df) is less than end_day.
        if len(per_fund_df) < end_day:
            print('***INFO: len(per_fund_df) = %s < %s' %(len(per_fund_df), end_day))
            end_day = len(per_fund_df)
            print('***INFO: end_day change to %s' %(end_day))

        sci_df      = sci_df_original[:end_day]
        per_fund_df = per_fund_df[:end_day]
        # print('end_day = %s' %(end_day))
        # print('len(sci_df.index) = %s' %(len(sci_df.index)))
        # print('len(per_fund_df.index) = %s' %(len(per_fund_df.index)))

        # The index in sci_d and per_fund_df is the date.
        # is_accuracy is True means that the days in these two df must be the same.
        # is_accuracy is False means that the days do not need to be the same, 
        # but the number of days need to be equal.
        if is_accuracy:
            if (sci_df.index != per_fund_df.index).any():
                print('***INFO: sci_df.index != per_fund_df.index')

                # Most cases are that the per_fund_df data is one day latter than sci_df data,
                # So move SCI one day back.
                # For the case that the fund data is more latter than SCI data(more than one day), I don't deal with it right now,
                # because the number of these cases is less than 10.
                if sci_df.index[0] != per_fund_df.index[0] and len(sci_df.index) > 1 and sci_df.index[1] == per_fund_df.index[0]:
                    sci_df = sci_df_original[1 : end_day + 1]

                num = -1
                for element in (sci_df.index != per_fund_df.index):
                    num = num +1
                    if (str(element) == 'True'):
                        end_day = num

                        sci_df       = sci_df[:end_day]
                        per_fund_df  = per_fund_df[:end_day]
                        print('***INFO: end_day change to %s' %(end_day))
                        break

            if end_day == 0:
                print('!!!!!! Skip this fund because end_day == 0!!!!!!')

                # Skip this fund, but I end_day to 1 here to let the process
                # going. Then filter out these funds when save information to file.
                end_day = 1
                skipped_funds.append(fund_symbol)

        if is_analysis_label:
            pass
        else:
            # Collect data that is used for training model.
            start_day = dy.TWO_MONTHS
            if len(per_fund_df) <= start_day:
                if fund_symbol not in skipped_funds:
                    skipped_funds.append(fund_symbol)
                continue
            else:
                per_fund_df = per_fund_df[start_day:]
                sci_df = sci_df[start_day:]


        rate_of_rise_days       = round(len(per_fund_df[per_fund_df.change > 0])/(end_day - start_day), 4)
        rate_of_higher_than_sci = round(len(per_fund_df[per_fund_df.change > sci_df.p_change])/(end_day - start_day), 4)

        temp_per_fund = per_fund_df[per_fund_df.change > 0]
        temp_sci_df   = sci_df[per_fund_df.change > 0]
        temp_per_fund = temp_per_fund[temp_per_fund.change > temp_sci_df.p_change]
        rate_of_rise_and_higher_than_sci = round(len(temp_per_fund)/(end_day - start_day), 4)

        fund_symbols_list.append(fund_symbol)
        days_of_processed_list.append((end_day - start_day))
        rate_of_rise_days_list.append(rate_of_rise_days)
        rate_of_higher_than_sci_list.append(rate_of_higher_than_sci)
        rate_of_rise_and_higher_than_sci_list.append(rate_of_rise_and_higher_than_sci)


    fund_dict = {
                    'symbol' : fund_symbols_list,
                    'days_of_processed' : days_of_processed_list,
                    'rate_of_rise_days' : rate_of_rise_days_list,
                    'rate_of_higher_than_sci' : rate_of_higher_than_sci_list,
                    'rate_of_rise_and_higher_than_sci' : rate_of_rise_and_higher_than_sci_list
                }
    fund_df = pd.DataFrame(fund_dict,
                            columns = ['symbol',
                                        'days_of_processed',
                                        'rate_of_rise_days',
                                        'rate_of_higher_than_sci',
                                        'rate_of_rise_and_higher_than_sci']
                            )

    fund_df = fund_df.set_index('symbol')

    print('Skip %d funds are:\n %s' %(len(skipped_funds), skipped_funds))

    # if is_analysis_label:
    #     output_file_long_path, = r'.\data\processingdata\mix_fund_hist_analysis_label.csv'
    # else:
    #     output_file_long_path, = r'.\data\processingdata\mix_fund_hist.csv'

    print('Saving %s funds to %s' %(len(fund_df), output_file_long_path,))
    fund_df.to_csv(output_file_long_path, encoding = 'utf-8')

    return fund_df


def AddIncreaseAttributes(file_long_path):
    '''
    Add the increase of several months in the input file.

    Inputs:
        file_long_path - file that contains the information of fund and will be added
                         in Increasement of fund.
    '''
    print('Calculating the amount of increase...')
    days_list = [dy.ONE_MONTH, dy.TWO_MONTHS, dy.THREE_MONTHS, dy.FOUR_MONTHS, dy.FIVE_MONTHS, dy.SIX_MONTHS]
    
    # file_long_path = r'.\data\processingdata\mix_fund_hist.csv'
    fund_df = pd.read_csv(file_long_path, encoding = 'utf-8', dtype = {'symbol' : str})
    fund_df = fund_df.set_index('symbol')

    file_short_path = r'.\data\processingdata\fund\historyinfo'
    for fund_symbol in fund_df.index.values[:]:
        print('Calculating fund %s increasement...' %(fund_symbol))

        fund_long_path = file_short_path + dy.sep  + str(fund_symbol) + '.csv'
        per_fund_df = pd.read_csv(fund_long_path)

        increase_of_one_month    = None
        increase_of_two_months   = None
        increase_of_three_months = None
        increase_of_four_months  = None
        increase_of_five_months  = None
        increase_of_six_months   = None

        if len(per_fund_df.index) >= days_list[5]:
            increase_of_six_months = round((per_fund_df.iloc[0].total - per_fund_df.iloc[days_list[5]-1].total) / per_fund_df.iloc[days_list[5]-1].total, 4)

        if len(per_fund_df.index) >= days_list[4]:
            increase_of_five_months = round((per_fund_df.iloc[0].total - per_fund_df.iloc[days_list[4]-1].total) / per_fund_df.iloc[days_list[4]-1].total, 4)

        if len(per_fund_df.index) >= days_list[3]:
            increase_of_four_months = round((per_fund_df.iloc[0].total - per_fund_df.iloc[days_list[3]-1].total) / per_fund_df.iloc[days_list[3]-1].total, 4)

        if len(per_fund_df.index) >= days_list[2]:
            increase_of_three_months = round((per_fund_df.iloc[0].total - per_fund_df.iloc[days_list[2]-1].total) / per_fund_df.iloc[days_list[2]-1].total, 4)

        if len(per_fund_df.index) >= days_list[1]:
            increase_of_two_months = round((per_fund_df.iloc[0].total - per_fund_df.iloc[days_list[1]-1].total) / per_fund_df.iloc[days_list[1]-1].total, 4)

        if len(per_fund_df.index) >= days_list[0]:
            increase_of_one_month = round((per_fund_df.iloc[0].total - per_fund_df.iloc[days_list[0]-1].total) / per_fund_df.iloc[days_list[0]-1].total, 4)

        fund_df.loc[fund_symbol, 'increase_of_one_month']    = increase_of_one_month
        fund_df.loc[fund_symbol, 'increase_of_two_months']   = increase_of_two_months
        fund_df.loc[fund_symbol, 'increase_of_three_months'] = increase_of_three_months
        fund_df.loc[fund_symbol, 'increase_of_four_months']  = increase_of_four_months
        fund_df.loc[fund_symbol, 'increase_of_five_months']  = increase_of_five_months
        fund_df.loc[fund_symbol, 'increase_of_six_months']   = increase_of_six_months

    fund_df.to_csv(file_long_path, index_col = 'symbol', encoding = 'utf-8')
    print('Save to file %s' %(file_long_path))


def GenerateLabelFundsToFiles(file_long_path):
    '''
    Choose which funds we can give a label and then save them to file.

    Inputs:
        file_long_path - file that contains the funds data.
    '''
    # file_long_path = r'.\data\processingdata\mix_fund_hist_analysis_label.csv'
    fund_df = pd.read_csv(file_long_path, encoding = 'utf-8', dtype = {'symbol' : str}).set_index('symbol')
    print('Read %s' %(file_long_path))
    print('Total funds %s' %(len(fund_df)))

    # del fund_df['label_past_one_and_two_month']

    # Drop the outlier data.
    fund_df = fund_df[fund_df.days_of_processed > 100]
    fund_df = fund_df[fund_df.increase_of_one_month < 0.3]
    fund_df = fund_df[fund_df.increase_of_three_months < 0.5]

    print('Process %s funds' %(len(fund_df)))

    # Choose the candidates that will be labeled.
    num_of_candidates = 100

    fund_df_tmp = fund_df.sort_values(by = 'increase_of_two_months', ascending = False)
    cutting_value_of_two_months = fund_df_tmp.iloc[num_of_candidates]['increase_of_two_months']
    print('The increasement of the %sth fund in past two months is %s' %(num_of_candidates, cutting_value_of_two_months))

    fund_df_tmp = fund_df.sort_values(by = 'increase_of_one_month', ascending = False)
    cutting_value_of_one_month = fund_df_tmp.iloc[num_of_candidates]['increase_of_one_month']
    print('The increasement of the %sth fund in past one month is %s' %(num_of_candidates, cutting_value_of_one_month))

    # Filter criteria.
    fund_df_tmp = fund_df_tmp[fund_df_tmp.increase_of_one_month > cutting_value_of_one_month]
    print('Top %s funds in past one month is %s' %(num_of_candidates, len(fund_df_tmp)))
    fund_df_tmp = fund_df_tmp[fund_df_tmp.increase_of_two_months > cutting_value_of_two_months]
    print('Top %s funds in past one AND two month is %s' %(num_of_candidates, len(fund_df_tmp)))
    fund_df_tmp = fund_df_tmp[fund_df_tmp.increase_of_two_months > fund_df_tmp.increase_of_one_month * 1.0]
    print('Top %s funds in past one AND two month and increase_of_two_months > increase_of_one_month*0.8 is %s' %(num_of_candidates, len(fund_df_tmp)))

    print('Label %d funds: %s' %(len(fund_df_tmp), fund_df_tmp.index.values))
    labeled_fund_file = r'.\data\processingdata\mix_fund_label_past_one_and_two_month.csv'
    fund_df_tmp.to_csv(labeled_fund_file, encoding = 'utf-8', index_col = 'symbol')
    print('Saved %s with labels' %(labeled_fund_file))

    #------------------------------------------------------------------------#
    fund_df_tmp = fund_df.sort_values(by = 'increase_of_one_month', ascending = False)
    cutting_value_of_one_month = fund_df_tmp.iloc[num_of_candidates]['increase_of_one_month']
    print('The increasement of the %sth fund in past one month is %s' %(num_of_candidates, cutting_value_of_one_month))

    # Filter criteria.
    fund_df_tmp = fund_df_tmp[fund_df_tmp.increase_of_one_month > cutting_value_of_one_month] 
    # fund_df_tmp = fund_df_tmp[fund_df_tmp.increase_of_two_months > cutting_value_of_two_months]
    # fund_df_tmp = fund_df_tmp[fund_df_tmp.increase_of_two_months > fund_df_tmp.increase_of_one_month * 0.8]

    print('Label %d funds: %s' %(len(fund_df_tmp), fund_df_tmp.index.values))
    labeled_fund_file = r'.\data\processingdata\mix_fund_label_past_one_month_only.csv'
    fund_df_tmp.to_csv(labeled_fund_file, encoding = 'utf-8', index_col = 'symbol')
    print('Saved %s with labels' %(labeled_fund_file))


def AddLabelAttributes(file_long_path, label_file_long_path, label_name):
    # Read the file that will add label in.
    # file_long_path = r'.\data\processingdata\mix_fund_hist.csv'
    fund_df = pd.read_csv(file_long_path, encoding = 'utf-8', dtype = {'symbol' : str}).set_index('symbol')
    print('Read %s' %(file_long_path))
    print('Total funds %s' %(len(fund_df)))

    # This is the file that contains all the labeld funds.
    label_fund_df = pd.read_csv(label_file_long_path, encoding = 'utf-8', dtype = {'symbol' : str}).set_index('symbol')
    print('Read %s' %(label_file_long_path))
    print('Total labeled funds %s' %(len(label_fund_df)))

    for fund_symbol in label_fund_df.index.values:
        (fund_df.loc[fund_symbol, label_name]) = 1

    fund_df[label_name].fillna(0, inplace = True)
    fund_df[label_name] = fund_df[label_name].astype(int)

    fund_df.to_csv(file_long_path, encoding = 'utf-8', index_col = 'symbol')
    print('Saved %s with labels' %(file_long_path))


def MakeTopFundsPlot(file_long_path):
    fund_df = pd.read_csv(file_long_path, encoding = 'utf-8', dtype = {'symbol' : str}).set_index('symbol')

    # Drop the outlier data.
    fund_df = fund_df[fund_df.days_of_processed > 100]
    fund_df = fund_df[fund_df.increase_of_one_month < 0.3]
    fund_df = fund_df[fund_df.increase_of_three_months < 0.5]    

    print('Proccess %s funds.' %(len(fund_df)))

    top_num = 50
    fund_df_tmp1 = fund_df.sort_values(by = 'increase_of_one_month', ascending = False)[:top_num]
    _makeSubplot(plot_loc = 231, fund_df = fund_df_tmp1, months = 1)

    fund_df_tmp2 = fund_df.sort_values(by = 'increase_of_two_months', ascending = False)[:top_num]
    _makeSubplot(plot_loc = 232, fund_df = fund_df_tmp2, months = 2)

    fund_df_tmp3 = fund_df.sort_values(by = 'increase_of_three_months', ascending = False)[:top_num]
    _makeSubplot(plot_loc = 233, fund_df = fund_df_tmp3, months = 3)

    fund_df_tmp4 = fund_df.sort_values(by = 'increase_of_four_months', ascending = False)[:top_num]
    _makeSubplot(plot_loc = 234, fund_df = fund_df_tmp4, months = 4)

    fund_df_tmp5 = fund_df.sort_values(by = 'increase_of_five_months', ascending = False)[:top_num]
    _makeSubplot(plot_loc = 235, fund_df = fund_df_tmp5, months = 5)

    fund_df_tmp6 = fund_df.sort_values(by = 'increase_of_six_months', ascending = False)[:top_num]
    _makeSubplot(plot_loc = 236, fund_df = fund_df_tmp6, months = 6)




    fund_basic = pd.read_csv('./data/processingdata/fund/mix_fund_basic.csv', encoding='utf-8', dtype={'symbol' : str}).set_index('symbol')

    symbols1 = fund_df_tmp1.index.values
    symbols2 = fund_df_tmp2.index.values
    symbols3 = fund_df_tmp3.index.values

    targets = list()
    for symbol in symbols1:
        if symbol in symbols2 and symbol in symbols3:
            if fund_basic.loc[symbol, 'fund_scale'] > 5:
                targets.append(symbol)


    print("targets: {}".format(targets))


    
    plt.show()


def _makeSubplot(plot_loc, fund_df, months = ''):
    plt.subplot(plot_loc)

    x_axis = [1, 2 ,3 ,4 ,5, 6]
    increasement_list = []
    for fund_symbol in fund_df.index.values:
        increasement = []
        increasement.append(fund_df.loc[fund_symbol,'increase_of_one_month'])
        increasement.append(fund_df.loc[fund_symbol,'increase_of_two_months'])
        increasement.append(fund_df.loc[fund_symbol,'increase_of_three_months'])
        increasement.append(fund_df.loc[fund_symbol,'increase_of_four_months'])
        increasement.append(fund_df.loc[fund_symbol,'increase_of_five_months'])
        increasement.append(fund_df.loc[fund_symbol,'increase_of_six_months'])

        plt.plot(x_axis, increasement) 
        plt.title('Top 25 in the past ' + str(months) + ' months')
        plt.xlabel('Months')
        plt.ylabel('Increasement')


def MergeFundData(file_long_path_hist, file_long_path_basic, output_file_long_path):
    print('Merging fund...')
    # file_long_path_hist = r'.\data\processingdata\mix_fund_hist.csv'
    fund_df_hist  = pd.read_csv(file_long_path_hist, encoding = 'utf-8', dtype = {'symbol' : str}).set_index('symbol')
    print('There are %s funds in %s' %(len(fund_df_hist), file_long_path_hist))

    # file_long_path_basic = r'.\data\processingdata\mixfund_basic.csv'
    fund_df_basic = pd.read_csv(file_long_path_basic, encoding = 'utf-8', dtype = {'symbol' : str}).set_index('symbol')
    print('There are %s funds in %s' %(len(fund_df_basic), file_long_path_basic))

    fund_df = pd.merge(fund_df_hist, fund_df_basic, left_index = True, right_index = True, how = 'inner')

    # file_long_path = r'.\data\processingdata\mix_fund_merged.csv'
    fund_df.to_csv(output_file_long_path, index_col = 'symbol', encoding = 'utf-8')
    print('Saved merged %s funds to %s' %(len(fund_df), output_file_long_path))

def GenerateLatestData():
    # Analysis the fund history data with ALL days that scraped from the net.
    # So that we can choose which funds we can buy and make the label.
    # This daa will be used to predict.
    process_fund_history_data(is_analysis_label = True,
        output_file_long_path = r'.\data\processingdata\mix_fund_hist_latest.csv')

    AddIncreaseAttributes(file_long_path = r'.\data\processingdata\mix_fund_hist_latest.csv')

    MergeFundData(  file_long_path_hist = r'.\data\processingdata\mix_fund_hist_latest.csv',
                    file_long_path_basic = r'.\data\processingdata\fund\mix_fund_basic.csv',
                    output_file_long_path = r'.\data\processingdata\mix_fund_merged_latest.csv')

def GenerateTrainingData():
    # Filter out the latest one or two months data, we cannot use these data to build the model.
    process_fund_history_data(is_analysis_label = False,
        output_file_long_path = r'.\data\processingdata\mix_fund_hist_training.csv')

    # Merge the history data and the basic data in one file.
    MergeFundData(  file_long_path_hist = r'.\data\processingdata\mix_fund_hist_training.csv',
                    file_long_path_basic = r'.\data\processingdata\fund\mix_fund_basic.csv',
                    output_file_long_path = r'.\data\processingdata\mix_fund_merged_training.csv')

    # Add the rate of increasement in the past several months in the input file.
    AddIncreaseAttributes(file_long_path = r'.\data\processingdata\mix_fund_merged_training.csv')

    # Generate label funds when analysis the funds, not in the training dataset generation stage.
    GenerateLabelFundsToFiles(file_long_path = r'.\data\processingdata\mix_fund_merged_training.csv')

    # Add label in the data file.
    AddLabelAttributes(file_long_path = r'.\data\processingdata\mix_fund_merged_training.csv',
                        label_file_long_path = r'.\data\processingdata\mix_fund_label_past_one_and_two_month.csv',
                        label_name = 'label_past_one_and_two_month')

    # Add label in the data file.
    AddLabelAttributes(file_long_path = r'.\data\processingdata\mix_fund_merged_training.csv',
                        label_file_long_path = r'.\data\processingdata\mix_fund_label_past_one_month_only.csv',
                        label_name = 'label_past_one_month_only')
   

if __name__ == "__main__":
    # MakeTopFundsPlot(r'.\data\processingdata\mix_fund_hist.csv')

    #GenerateLatestData()

    #GenerateTrainingData()


    MakeTopFundsPlot(r'.\data\processingdata\mix_fund_label_past_one_and_two_month.csv')
    MakeTopFundsPlot(r'.\data\processingdata\mix_fund_label_past_one_month_only.csv')

