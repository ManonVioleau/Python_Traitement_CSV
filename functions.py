import config
from urllib.request import urlopen
import json
from binance.client import Client
from binance.exceptions import BinanceAPIException

client_binance = Client(config.BINANCE_API_KEY, config.BINANCE_API_SECRET)

def get_value_in_USDT_width_binance(timestamp, currency, quantity):
    symbol = currency + 'USDT'
    startTime = timestamp
    endTime = timestamp + 60000
    try:
        price = client_binance.get_klines(symbol=symbol, interval='1m', startTime=startTime, endTime=endTime, limit=1)
    except BinanceAPIException:
        price = []

    if price == []:
        price = 0
    else:
        price = float(price[0][1])
    print(float(quantity) * float(price))
    return (float(quantity) * float(price))

def get_value_in_USD(date, quantity):
    date = str(date).split(' ')[0]
    access_key = config.EXCHANGERATESAPI_API_KEY
    url = 'http://api.exchangeratesapi.io/v1/' + date + '?access_key=' + access_key + '&base=EUR&symbols=USD'
    
    response = urlopen(url)

    data_json = json.loads(response.read())

    result = float(data_json['rates']['USD']) * float(quantity)

    return result
