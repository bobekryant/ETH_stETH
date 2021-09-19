/*
This code will pull events from the ETH-stETH curve pool and save them as txt files
TokenExchange, TokenExchangeUnderlying, AddLiquidity, RemoveLiquidityImbalance, RemoveLiquidity, and RemoveLiquidityOne events are recorded and saved under STETH_ETH_events.txt
RampA events are recorded and saved under STETH_ETH_ramp.txt

To use, insert infura key in .env file
*/

const dotenv = require("dotenv");
dotenv.config();
const path = require('path')
var fs = require('fs');
var Web3 = require("web3");
const infura_provider = process.env.INFURAPROVIDER;
const web3 = new Web3(infura_provider);
const curveJson = require("../ABI/Curve.json");
const curveAbi = curveJson.abi;
const curveAddress = "0xDC24316b9AE028F1497c275EB9192a3Ea0f67022";
const curveContract = new web3.eth.Contract(curveAbi, curveAddress);

let beginning = 11594223; //pool starting block
let end = 11599223; //current ending block
let each = 1000; //iterations of 1k
let current_block = 0;


function sleep(milliseconds){
  /*
  Infra cannot handle rapid requests. 
  Function to Pause
  */
  const date = Date.now();
  let currentDate = null;
  do {
    currentDate = Date.now();
  } while (currentDate - date < milliseconds);
}

const dir_path = require('path').dirname(require.main.filename);
const data_path = require('path').dirname(dir_path);
const file_loc_events = path.join(data_path,'data','STETH_ETH_events.txt');
const file_loc_ramp = path.join(data_path, 'data', 'STETH_ETH_ramp.txt');
var logger = fs.createWriteStream(file_loc_events); //store add liquidity, remove liquidity, swaps
logger.write('timestamp,blocknumber,event,eth_amount,steth_amount,walletid,transactionHash'+ "\n");
var logger_ramp = fs.createWriteStream(file_loc_ramp); // store 'A' parameter changes
logger_ramp.write('timestamp,blocknumber,event,old_a,new_a,initial_time,future_time'+ "\n");


function run(){
  /*
  Pulls event data with while loop on block time
  */
  while (current_block < end) {
    current_block = beginning + each
      if ( current_block <= end ){
        current_block=current_block;
      }
      else {
        current_block=end;
      }
      curveContract.getPastEvents("allEvents", {fromBlock: beginning,toBlock: current_block,}).
      then(async (events) => test(events, logger, logger_ramp));
      beginning = beginning +each;
      sleep(100);
    }
}

async function test(event, logger, logger_ramp){
  /*
  Writes events to txt file
  */
  for (const index in event){ // iterate each event
    var blockNumber = event[index]['blockNumber'];
    var tx_hash = event[index]['transactionHash'];
    var event_name = event[index]['event'];
    var blockData = await web3.eth.getBlock(blockNumber)
    var time_stamp = blockData.timestamp
    var value = await web3.eth.getTransaction(tx_hash);
    var wallet_id = value['from']

    // logic to convert event to row in txt file
    if (event_name == 'TokenExchange' | event_name == 'TokenExchangeUnderlying') {
      if (event[index]['returnValues']['bought_id'] == '0'){
        logger.write(time_stamp + ','+ blockNumber+','+event_name + ',' + '-'+ event[index]['returnValues']['tokens_bought'] + ',' + event[index]['returnValues']['tokens_sold'] + ',' + wallet_id + ',' +tx_hash + "\n")
      }
      else{
        logger.write(time_stamp + ','+ blockNumber+','+event_name + ',' + event[index]['returnValues']['tokens_sold'] + ',' + '-' + event[index]['returnValues']['tokens_bought'] + ',' + wallet_id + ',' + tx_hash + "\n")
      }
      }
      
      else if (event_name == 'AddLiquidity') {
        logger.write(time_stamp + ','+ blockNumber+','+event_name + ',' + event[index]['returnValues']['token_amounts'][0] + ',' + event[index]['returnValues']['token_amounts'][1] + ',' + wallet_id + ',' + tx_hash + "\n")
      }

      else if (event_name == 'RemoveLiquidityImbalance' | event_name == 'RemoveLiquidity') {
        logger.write(time_stamp + ','+ blockNumber+','+event_name + ',' + '-'+ event[index]['returnValues']['token_amounts'][0] + ',' + '-' + event[index]['returnValues']['token_amounts'][1] + ',' + wallet_id + ',' + tx_hash + "\n")
      } 
  
      else if (event_name == 'RemoveLiquidityOne') {
        ticker_index = value['input'].slice(10).slice(64, 128).substr(-1); // tells which index the liquidity is being removed to, 0 is ETH, 1 is stETH
        if (ticker_index == "0"){
          logger.write(time_stamp + ','+ blockNumber+','+event_name + ',' + '-'+ event[index]['returnValues']['coin_amount'] + ',' + "0" + ',' + wallet_id + ',' + tx_hash + "\n")
        }
        else{
          logger.write(time_stamp + ','+ blockNumber+','+event_name + ',' +  "0" + ',' + '-'+ event[index]['returnValues']['coin_amount'] + ',' + wallet_id + ',' + tx_hash + "\n")
          }
        }
      else if (event_name == 'RampA') {
        logger_ramp.write(time_stamp + ','+ blockNumber+','+event_name + ',' + event[index]['returnValues']['old_A'] + event[index]['returnValues']['new_A'] + event[index]['returnValues']['initial_time'] + event[index]['returnValues']['future_time'] + ',' +tx_hash + "\n")
      } 

      else { //error handling... unexpected event
          console.log(event[index])
          throw new Error(event['event'] +' How to parse?');
          } 

  }
}


run()
console.log("done")


