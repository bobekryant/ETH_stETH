"""
Curve Parsing STETH ETH pool from txt file... turn to readable data to analyze
Save daily and hourly files

Defi apis contains the alchmey key for an archive node

"""

import pandas as pd
import numpy as np
import datetime as dt
import os

from web3 import Web3
import requests
import json
import defi_apis # add own alchemy key
import time


# set up pool smart contract
# connect with node
pool_addy = "0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"
w3 = Web3(Web3.WebsocketProvider(defi_apis.alchemy_key)) 
contract_address = w3.toChecksumAddress(pool_addy)
curve_abi_url = 'https://api.etherscan.io/api?module=contract&action=getabi&address='+pool_addy
r = requests.post(curve_abi_url)
curve_abi = json.loads(r.text)['result']
contract = w3.eth.contract(abi=curve_abi, address=contract_address)

if __name__ == '__main__':
    
    #load and clean raw data
    data_dir = os.path.join(os.getcwd(),'data')
    data = pd.read_csv(os.path.join(data_dir,'STETH_ETH_events.txt'),index_col=0)
    data.index = [dt.datetime.utcfromtimestamp(ts) for ts in data.index]
    data['eth_amount'] = data['eth_amount'].apply(lambda x: int(x)/1e18)
    data['steth_amount'] = data['steth_amount'].apply(lambda x: int(x)/1e18)
    data.sort_index(inplace=True)
    
    #### get events
    trades = data[data['event']=='TokenExchange']
    trades['price'] = abs(trades['eth_amount'] /trades['steth_amount'])
    trades['Volume'] = trades['eth_amount']
    trades = trades[trades['price']!=0] #send so little eth output gets rounded down
    data = pd.merge(data,trades[['price','Volume']],left_index=True,right_index=True,how='outer')
    data['price'] = data['price'].fillna(method='ffill')
    data['Volume'].fillna(0,inplace=True)
    data['Volume'] = abs(data['Volume'])
    data.to_csv(os.path.join(data_dir,'events_ETH_STETH_pool.csv'))
    
    
    #### get daily pool
    data['Volume'].fillna(0,inplace=True)
    daily_data = data[['blocknumber','event','price']].resample('1d').last()
    daily_data['Volume'] = abs(data[['Volume']]).resample('1d').sum()
    
    daily_data['ETH_Pool'] = np.nan
    daily_data['STETH_Pool'] = np.nan
    
    for index,row in daily_data.iterrows(): 
        daily_data.loc[index,'ETH_Pool'] = contract.functions.balances(0).call(block_identifier=row['blocknumber'])/1e18
        daily_data.loc[index,'STETH_Pool'] = contract.functions.balances(1).call(block_identifier=row['blocknumber'])/1e18
    daily_data.to_csv(os.path.join(data_dir,'daily_ETH_STETH_pool.csv'))
    
    #### get hourly pool... make sure node has high enough api limit... may need to rest
    hourly_data = data[['blocknumber','event','price']].resample('1h').last()
    hourly_data[['blocknumber','event','price']] = hourly_data[['blocknumber','event','price']].fillna(method='ffill')
    hourly_data['Volume'] = abs(data[['Volume']]).resample('1h').sum()
    hourly_data['ETH_Pool'] = np.nan
    hourly_data['STETH_Pool'] = np.nan
    
    for index,row in hourly_data.iterrows(): 
        if np.isnan(hourly_data.loc[index,'ETH_Pool']):
            hourly_data.loc[index,'ETH_Pool'] = contract.functions.balances(0).call(block_identifier=int(row['blocknumber']))/1e18
        if np.isnan(hourly_data.loc[index,'STETH_Pool']):
            hourly_data.loc[index,'STETH_Pool'] = contract.functions.balances(1).call(block_identifier=int(row['blocknumber']))/1e18
            time.sleep(1)
    hourly_data.to_csv(os.path.join(data_dir,'hourly_ETH_STETH_pool.csv'))
