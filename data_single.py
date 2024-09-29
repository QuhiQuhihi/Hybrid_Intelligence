import os
import sys
import warnings

import numpy as np
import pandas as pd

# this class is to preprocess data from FRED 
class data_prep:

    def __init__(self, base_path, ticker, term, pred_delta, corr_window):
    
        # set directory for data preparation
        self.base_path = base_path
        self.data_path = self.base_path + 'data/'
        self.stock_path = self.data_path + 'price/'
        self.rate_path = self.data_path + 'rate/'
        self.cds_path = self.data_path + 'cds/'
        self.financial_path = self.data_path + 'financial/'
        self.fcf_path = self.financial_path + 'freecashflow/'
        self.growth_path = self.financial_path + 'growth_rate/'
        self.input_path = self.data_path + 'input/'

        self.ticker_1 = ticker
        # self.ticker_2 = ticker2
        self.term = term
        self.pred_delta = pred_delta
        self.corr_window = corr_window

        # which is time interval for return calculation - day, week, month
        self.shift_delta = 1 # time delta for return calculation - use daily return


        # call final function (integrate)
        self.df_input = self.integrate()
        # print(self.df_input)

    def integrate(self):
        # this is final function for this class
        # if input data looks missing, take a look this function
        stock = self.stock_prep()
        cds = self.cds_prep()
        rate = self.rate_prep()
        fcf = self.fcf_prep()
        growth = self.growth_prep()

        # df_input = stock[['corr_window_'+str(self.corr_window),'Adj Return Delta '+str(self.shift_delta)+'_' + 'tickerA' , 'Adj Return Delta '+str(self.shift_delta)+'_' + 'tickerB']]
        
        df_input = stock
        df_input = df_input.join(cds, how='left')
        df_input = df_input.join(rate, how='left')

        df_input = df_input.ffill() # especially this !!!
        df_input = df_input.dropna() # especially this !!!
        
        df_input = df_input.merge(fcf['Levered_FCF_1_year'], left_index=True, right_index=True, how='left')

        df_input = df_input.ffill() # especially this !!!
        df_input = df_input.dropna() # especially this !!!

        df_input = df_input.merge(growth['Perpetuity_Growth'], left_index=True, right_index=True, how='left')

        df_input = df_input.ffill() # especially this !!!
        df_input = df_input.dropna() # especially this !!!

        df_input.to_csv(self.input_path + 'df_input.csv')

        df_input = df_input[[
            'Adj Return Delta '+str(self.shift_delta),
            'CDS Premium', 'CDS Spread',
            'CDS Premium Change Delta '+str(self.shift_delta),
            'Riskfree',
            'Riskfree_change',
            'Levered_FCF_1_year',
            'Perpetuity_Growth'
        ]]

        df_input.to_csv(self.input_path + 'df_input.csv')

        return df_input # this is what we are going to use

    def stock_prep(self):
        # Getting data for stock 1
        price_1_file = pd.read_csv(self.stock_path+self.ticker_1+".csv", index_col=0)
        price_1_file['Adj Return Delta '+str(self.shift_delta)] = np.log(price_1_file['Adj Close'] / price_1_file['Adj Close'].shift(self.shift_delta))
        price_1_file.index = pd.to_datetime(price_1_file.index)
        price_1_file = price_1_file.fillna(0)

        df_price = price_1_file['Adj Return Delta '+str(self.shift_delta)].to_frame()
        

        return df_price
    

    def cds_prep(self):
        # Getting CDS data and change in CDS premium for company 1
        cds_1_file = pd.read_excel(self.cds_path+"CDS_"+self.ticker_1+".xls", sheet_name="data", index_col=0)
        cds_1_file['bid-ask'] = cds_1_file['ask'] - cds_1_file['bid']
        cds_1_file['CDS Premium'] = cds_1_file['mid'] / 10000
        cds_1_file['CDS Spread'] = (cds_1_file['ask'] - cds_1_file['bid']) / 10000

        cds_1_file['CDS Premium Change Delta '+str(self.shift_delta)] = cds_1_file['mid'] - cds_1_file['mid'].shift(self.shift_delta)
        # cds_1_file['CDS Spread Change Delta '+str(self.shift_delta)] = cds_1_file['CDS Spread'] - cds_1_file['CDS Spread'].shift(self.shift_delta)

        df_cds = cds_1_file[[
            'CDS Premium', 'CDS Spread',
            'CDS Premium Change Delta '+str(self.shift_delta),
            # 'CDS Spread Change Delta '+str(self.shift_delta)
            ]]
        df_cds = df_cds.ffill() # ffil if daily data is missing - it can happen in illiquid market
        df_cds = df_cds.dropna() # just in case
        
        df_cds.to_csv(self.input_path + 'df_cds.csv')
        return df_cds
    
    def rate_prep(self):
        # use data from FRED --> search below page in FRED
        # Market Yield on U.S. Treasury Securities at 3-Month Constant Maturity, Quoted on an Investment Basis
        rate_file = pd.read_excel(self.rate_path+"Market Yield on U.S. Treasury Securities at 3-Month Constant Maturity, Quoted on an Investment Basis ("+ self.term +").xls"
                            ,index_col=0, skiprows=10, na_values=[0])
        rate_file.index = pd.to_datetime(rate_file.index)
        rate_file = rate_file.ffill()

        rate_file = rate_file[[self.term]] / 100 # make it decimal
        rate_file = rate_file.rename(columns={self.term:"Riskfree"}) # changed

        futures_file = pd.read_csv(self.rate_path+"US_Treasury_Bond_Futures_September.csv", index_col=0)
        futures_file.index = pd.to_datetime(futures_file.index)

        futures_file['Riskfree_change'] = np.log(futures_file['Adj Close'] / futures_file['Adj Close'].shift(self.shift_delta))
        futures_file.index = pd.to_datetime(futures_file.index)
        futures_file = futures_file.fillna(0)

        df_rate = rate_file.loc[:,'Riskfree'].to_frame().join(futures_file.loc[:,'Riskfree_change'].to_frame(), how='left')


        # sometimes, riskfree rate was zero.... divide by zero makes error. So use subtraction instead
        # df_rate['Riskfree Change Delta '+str(self.shift_delta)] = df_rate['Riskfree'] - df_rate['Riskfree'].shift(self.shift_delta)
        df_rate = df_rate.ffill() # ffil if daily data is missing - it can happen in illiquid market
        df_rate = df_rate.dropna()

        df_rate.to_csv(self.input_path + 'df_rate.csv')
        return df_rate
    
    def macro_prep(self):
        # Get macro economic data release date and index value data
        cpi_file = pd.read_csv(self.data_path+"cpi.csv", index_col=0)
        df_cpi = cpi_file[['CPI_YoY']]
        df_cpi.index = pd.to_datetime(df_cpi.index)

        unemp_file = pd.read_csv(self.data_path+"unemploy.csv", index_col=0)
        df_unemploy = unemp_file[['unemploy']]
        df_unemploy.index = pd.to_datetime(df_unemploy.index)

        # announcement date in list format
        cpi_announce_date = df_cpi.index.unique()
        unemp_announce_date = df_unemploy.index.unique()

        return df_cpi, df_unemploy
    
    def fcf_prep(self):
        ##################################################################################################
        # fcf_ticker_1 = pd.read_excel(self.fcf_path + self.ticker_1 + '_fcf_earning.xlsx', index_col=0)
        # # fcf_ticker_2 = pd.read_excel(self.fcf_path + self.ticker_2 + '_fcf_earning.xlsx', index_col=0)

        # fcf_ticker_1[self.ticker_1 + '_Levered_FCF_1_year'] = fcf_ticker_1['Levered_FCF'].rolling(4).mean()
        # # fcf_ticker_2[self.ticker_2 + '_Levered_FCF_1_year'] = fcf_ticker_2['Levered_FCF'].rolling(4).mean()

        # # if one year freecash flow is negative, plug 1 million as FCF. --> to avoid error
        # fcf_ticker_1[self.ticker_1 + '_Levered_FCF_1_year'] = fcf_ticker_1[self.ticker_1 + '_Levered_FCF_1_year'].apply(lambda x: 1 if x < 0 else x)
        # # fcf_ticker_2[self.ticker_2 + '_Levered_FCF_1_year'] = fcf_ticker_2[self.ticker_2 + '_Levered_FCF_1_year'].apply(lambda x: 1 if x < 0 else x)
        # ##################################################################################################

        fcf_ticker_1 = pd.read_excel(self.fcf_path + self.ticker_1 + '_fcf_earning.xlsx', index_col=0)
        fcf_ticker_1 = fcf_ticker_1.dropna()
        fcf_ticker_1['Levered_FCF_1_year'] = fcf_ticker_1['Levered_FCF'].rolling(4).mean()
        # fcf_ticker_1['Levered_FCF_1_year'] = fcf_ticker_1['Levered_FCF_1_year'].apply(lambda x: 1 if x < 0 else x)
        fcf_ticker_1 = fcf_ticker_1.dropna()

        return fcf_ticker_1

    def growth_prep(self):

        growth_ticker_1 = pd.read_excel(self.growth_path + self.ticker_1 + '_growth.xlsx', index_col=0)
        growth_ticker_1 = growth_ticker_1.dropna()
        growth_ticker_1['Perpetuity_Growth'] = growth_ticker_1['Perpetuity_Growth'].apply(lambda x: 0 if x < 0 else x)

        growth_ticker_1 = growth_ticker_1[['Perpetuity_Growth']]

        return growth_ticker_1
