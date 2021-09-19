"""
Curve Parsing STETH ETH pool from txt file... turn to readable data to analyze
Save daily and hourly files

Defi apis contains the alchmey key for an archive node
Add in eth balance, steth balance, and total_supply to calculate curve prices
"""

import pandas as pd
import numpy as np
import datetime as dt
import os
import time

from web3 import Web3
import requests
import json
import defi_apis # add own alchemy key
import time
import curve_pool


# curve_abi = pd.read_json(r'C:\Users\eric_\Documents\code\crypto\defi\dev\eth_steth_curve\ABI\Curve.json')
# set up pool smart contract
# connect with node
pool_addy = "0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"
w3 = Web3(Web3.WebsocketProvider(defi_apis.alchemy_key)) 
contract_address = w3.toChecksumAddress(pool_addy)
curve_abi_url = 'https://api.etherscan.io/api?module=contract&action=getabi&address='+pool_addy
r = requests.post(curve_abi_url)
curve_abi = json.loads(r.text)['result']
contract = w3.eth.contract(abi=curve_abi, address=contract_address)

lp_addy = '0x06325440d014e39736583c165c2963ba99faf14e'
lp_address = w3.toChecksumAddress(lp_addy)
lp_abi_url = 'https://api.etherscan.io/api?module=contract&action=getabi&address='+lp_addy
r = requests.post(lp_abi_url)
lp_abi = json.loads(r.text)['result']
lpTokenContract = w3.eth.contract( address=lp_address, abi=lp_abi)

if __name__ == '__main__':
    
    #load and clean raw data
    data_dir = os.path.join(os.getcwd(),'data')
    data = pd.read_csv(os.path.join(data_dir,'STETH_ETH_events.txt'),index_col=0)
    data.index = [dt.datetime.utcfromtimestamp(ts) for ts in data.index]
    data['eth_amount'] = data['eth_amount'].apply(lambda x: int(x)/1e18)
    data['steth_amount'] = data['steth_amount'].apply(lambda x: int(x)/1e18)
    data.sort_index(inplace=True)
    amp1_time_end = dt.datetime(2021,2,25)
    amp2_time_end = dt.datetime(2021,5,20)

    #### get events
    trades = data[data['event']=='TokenExchange']
    trades['price'] = abs(trades['eth_amount'] /trades['steth_amount'])
    trades['Volume'] = trades['eth_amount']
    trades = trades[trades['price']!=0] #send so little eth output gets rounded down
    data = pd.merge(data,trades[['price','Volume']],left_index=True,right_index=True,how='outer')
    data['price'] = data['price'].fillna(method='ffill')
    data['Volume'].fillna(0,inplace=True)
    data['Volume'] = abs(data['Volume'])

    data['eth_bal'] = np.nan
    data['steth_bal'] = np.nan
    data['total_supply'] = np.nan
    
    
    current_blk_num = 0
    final_blk = data['blocknumber'][-1]
    while current_blk_num<final_blk:
        try:
            file_loc = os.path.join(data_dir,'STETH_ETH_events_full.txt')
            if os.path.exists(file_loc):
                temp_file = pd.read_csv(file_loc,index_col=0,parse_dates=True)
                block_num_last = temp_file['blocknumber'][-1]
                outF = open(file_loc, "a")
            else:
                outF = open(file_loc, "a")
                outF.write('time,blocknumber,event,eth_amount,steth_amount,walletid,transactionHash,token_amount,price,Volume,eth_bal,steth_bal,total_supply')
                outF.write("\n")
                block_num_last = 0
            
        
            for i,row in data[data['blocknumber']>block_num_last].iterrows():
                current_blk_num = row['blocknumber']
                if (row['event']!='TokenExchange'):
                    if np.isnan(row['eth_bal']):
                        eth_bal = contract.functions.balances(0).call(block_identifier=int(row['blocknumber'])-1)/1e18
                        steth_bal = contract.functions.balances(1).call(block_identifier=int(row['blocknumber'])-1)/1e18
                        total_supply = lpTokenContract.functions.totalSupply().call(block_identifier=int(row['blocknumber'])-1)/1e18
                        row['eth_bal'] = eth_bal
                        row['steth_bal'] = steth_bal
                        row['total_supply'] = total_supply
                        time.sleep(1)
                line = str(row.name) + ',' + ','.join([str(ele) for ele in row])
                outF.write(line)
                outF.write("\n")
            outF.close()
        except:
            print(row)
            time.sleep(15)
            pass

    # data = pd.read_csv(os.path.join(data_dir,'STETH_ETH_events_full.txt'),index_col=0,parse_dates=True)
    # data.to_csv(os.path.join(data_dir,'events_ETH_STETH_pool.csv')).
    # num_skip = 7900
    # temp_file = pd.read_csv(file_loc,index_col=0,parse_dates=True,skiprows=[num_skip])[:num_skip]
    # temp_file.to_csv(file_loc)

    #### SINGLE SIDED TRADES
    data = pd.read_csv(os.path.join(data_dir,'STETH_ETH_events_full.txt'),index_col=0,parse_dates=True)
    data['eth_amount'] = data['eth_amount'].apply(lambda x: int(x) * 1e18)
    data['steth_amount'] = data['steth_amount'].apply(lambda x: int(x) * 1e18)
    data['eth_bal'] = [x if np.isnan(x) else float(x)* 1e18 for x in np.array(data['eth_bal'], dtype=float)]
    data['steth_bal'] = [x if np.isnan(x) else float(x)* 1e18 for x in np.array(data['steth_bal'], dtype=float)]
    data['total_supply'] = [x if np.isnan(x) else float(x)* 1e18 for x in np.array(data['total_supply'], dtype=float)]
    data['token_amount'] = [x if np.isnan(x) else float(x) for x in np.array(data['token_amount'], dtype=float)]
    data = data.loc[~data.index.duplicated(keep='first')]
    data['total_supply'] = data['total_supply'].fillna(method='ffill')
    data['total_supply_change'] = data['total_supply'].shift(-1).diff().abs() #its orignially lagged one so need to shift it foward to see the changes for current block

    data.loc[data['token_amount'].isnull(),'token_amount'] = data.loc[data['token_amount'].isnull(),'total_supply_change']
    data['token_amount'].fillna(0,inplace=True)

    data.sort_index(inplace=True)
    # remove_liq_trades = []
    for i,row in data.iterrows(): 
        if i<amp1_time_end:
            A = 5
        elif i<amp2_time_end:
            A = 10
        else:
            A = 50
        if row['token_amount']==0:
            continue
        pool = curve_pool.Curve(A,[row['eth_bal'],row['steth_bal']],2,tokens=row['total_supply']) #pool status before this block
        eth_percent = row['eth_bal'] / (row['steth_bal']+row['eth_bal'])
        steth_percent = row['steth_bal'] / (row['steth_bal']+row['eth_bal'])
        eth_out = row['token_amount']*eth_percent
        steth_out = row['token_amount']*steth_percent
        condition_1 =  (row['event']=='RemoveLiquidityOne')
        condition_2 = (row['event']=='AddLiquidity') and (row['eth_amount']==0 or row['steth_amount']==0)
        if condition_1 or condition_2:
            if row['eth_amount']!=0:
                trade_asset = 'steth'
                recieved = pool.exchange(1,0,steth_out)
                price = recieved/steth_out #eth/steth
                volume = recieved #in terms of eth
            else:
                trade_asset = 'eth'
                recieved = pool.exchange(0,1,eth_out)
                price = eth_out/recieved #eth/steth
                volume = eth_out #in terms of eth
            data.loc[i,'price'] = price
            data.loc[i,'Volume'] = volume/1e18
            # remove_liq_trades.append(data.loc[i])
    # test = pd.concat(remove_liq_trades,axis=1).T
    data.to_csv(os.path.join(data_dir,'events_ETH_STETH_pool.csv'))

    #############
    #### get daily pool
    data['Volume'].fillna(0,inplace=True)
    daily_data = data[['blocknumber','event','price','eth_bal','steth_bal']].resample('1d').last()
    daily_data['Volume'] = abs(data[['Volume']]).resample('1d').sum()
    daily_data[['eth_bal','steth_bal']] = daily_data[['eth_bal','steth_bal']]/1e18
    daily_data.rename({'eth_bal':'ETH_Pool','steth_bal':'STETH_Pool'},axis=1,inplace=True)
    daily_data.to_csv(os.path.join(data_dir,'daily_ETH_STETH_pool.csv'))
    
    #### get hourly pool... make sure node has high enough api limit... may need to rest
    hourly_data = data[['blocknumber','event','price','eth_bal','steth_bal']].resample('1h').last()
    hourly_data[['blocknumber','event','price']] = hourly_data[['blocknumber','event','price']].fillna(method='ffill')
    hourly_data['Volume'] = abs(data[['Volume']]).resample('1h').sum()
    hourly_data[['eth_bal','steth_bal']] = hourly_data[['eth_bal','steth_bal']]/1e18
    hourly_data.rename({'eth_bal':'ETH_Pool','steth_bal':'STETH_Pool'},axis=1,inplace=True)
    hourly_data.to_csv(os.path.join(data_dir,'hourly_ETH_STETH_pool.csv'))


    #### ANALYZE
    sub_frame = data[data['price']<0.966]
    sub_frame['Volume'].sum()
    
    
    sub_frame[sub_frame['event']=='TokenExchange']['Volume'].sum()
    sub_frame[sub_frame['event']=='RemoveLiquidityOne']['Volume'].sum()
    sub_frame[sub_frame['event']=='AddLiquidity']['Volume'].sum()

    sub_frame[sub_frame['event']=='RemoveLiquidityOne']['Volume'].describe()
