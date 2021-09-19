# -*- coding: utf-8 -*-
"""
Use Web3py to pull events
"""
from web3 import Web3
# from utils import create_contract
# from events import fetch_events
import itertools
import requests
import json
import defi_apis

w3 = Web3(Web3.WebsocketProvider(defi_apis.alchemy_key)) 
pool_addy = "0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"
contract_address = Web3.toChecksumAddress(pool_addy)
curve_abi_url = 'https://api.etherscan.io/api?module=contract&action=getabi&address='+pool_addy
r = requests.post(curve_abi_url)
curve_abi = json.loads(r.text)['result']
contract = w3.eth.contract(abi=curve_abi, address=contract_address)

from_block = 11594223
to_block = 12966697
block_loop = 1000
i = 0
start_block = from_block
all_events = []
while start_block<to_block:
    if i%100==0:
        print(i)
    events = list(fetch_events(contract.events.RemoveLiquidityOne, from_block=start_block, to_block = start_block+block_loop))
    all_events.append(events)
    start_block += block_loop

all_events = list(itertools.chain.from_iterable(all_events))
all_events[0]


tx = '0xd20e878f1f68612cf6ad9b4f34ed7e63a1ba073d7ad47cd4cd217c3fb188a84b'
for event in all_events if event['transactionHash']