import config
import pandas
from urllib.request import urlopen
import json
from binance.client import Client
from binance.exceptions import BinanceAPIException

client_binance = Client(config.BINANCE_API_KEY, config.BINANCE_API_SECRET)

data = pandas.read_excel(r'/home/manon/Bureau/Adrien/Adrien2/csv/coinbase_20171220_20190214.ods', engine="odf")
print(data)
new_datas = {'Portfolio' : [], 'Type' : [], 'Date' : [], 'Received Currency' : [], 'Received Quantity' : [], 'Sent Currency' : [], 'Sent Quantity' : [], 'Fee Currency' : [], 'Fee Quantity' : [], 'Trade value in USDT' : [], 'Fees value in USDT' : [], 'Total trade value in USDT' : [], 'Timestamp' : []}

new_datas['Portfolio'] = list(map(lambda x: 'Coinbase', data['Timestamp']))
new_datas['Type'] = list(map(lambda x:'Trade' if x == 'Buy' or x == 'Sell' else ('Withdrawal' if x == 'Send' else 'Deposit' ), data['Transaction Type']))
new_datas['Date'] = list(map(lambda x: x.split('T')[0] + ' ' + x.split('T')[1][:-1], data['Timestamp']))
new_datas['Received Currency'] = list(map(lambda x, y: x if (y == 'Buy' or y == 'Receive') else ('EUR' if y == 'Sell' else 0.0), data['Asset'], data['Transaction Type']))
new_datas['Received Quantity'] = list(map(lambda x, y, z: float(str(x).replace(',', '.')) if (y == 'Buy' or y == 'Receive') else (float(str(str(z).split(' ')[0]).replace(',', '.')) if y == 'Sell' else 0.0), data['Quantity Transacted'], data['Transaction Type'], data['Subtotal']))
new_datas['Sent Currency'] = list(map(lambda x, y: x if (y == 'Sell' or y == 'Send') else ('EUR' if y == 'Buy' else 0.0), data['Asset'], data['Transaction Type']))
new_datas['Sent Quantity'] = list(map(lambda x, y, z: float(str(x).replace(',', '.')) if (y == 'Sell' or y == 'Send') else (float(str(str(z).split(' ')[0]).replace(',', '.')) if y == 'Buy' else 0.0), data['Quantity Transacted'], data['Transaction Type'], data['Subtotal']))
new_datas['Fee Currency'] = list(map(lambda x: 'EUR' if x == 'Buy' or x == 'Sell' else 0.0, data['Transaction Type']))
new_datas['Fee Quantity'] = list(map(lambda x, y, z: float(str(z).split(' ')[0].replace(',', '.')) - float(str(x).split(' ')[0].replace(',', '.')) if float(str(z).split(' ')[0].replace(',', '.')) - float(str(x).split(' ')[0].replace(',', '.')) >= 0 and (y == 'Buy' or y == 'Sell') else 0.0, data['Subtotal'], data['Transaction Type'], data['Total (inclusive of fees)']))
new_datas['Timestamp'] = list(map(lambda x: (pandas.to_datetime(x) - pandas.Timestamp("1970-01-01 00:00:00")) // pandas.Timedelta('1ms'), new_datas['Date']))


def get_value_in_USD(date, quantity):
    date = str(date).split(' ')[0]
    access_key = config.EXCHANGERATESAPI_API_KEY
    url = 'http://api.exchangeratesapi.io/v1/' + date + '?access_key=' + access_key + '&base=EUR&symbols=USD'
    
    response = urlopen(url)

    data_json = json.loads(response.read())

    result = float(data_json['rates']['USD']) * float(quantity)

    return result

new_datas['Fees value in USDT'] = list(map(lambda x, y: 0 if y == 0 else get_value_in_USD(x, y), new_datas['Date'], new_datas['Fee Quantity']))
# new_datas['Fees value in USDT'] = list(map(lambda x: x, new_datas['Fee Quantity']))

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
    return (float(quantity) * float(price))


new_datas['Trade value in USDT'] = list(map(lambda timestamp, re_coin, re_quantity, se_coin, se_quantity: float(get_value_in_USDT_width_binance(timestamp, re_coin, re_quantity)) if re_coin != 'EUR' and re_coin != 0 else float(get_value_in_USDT_width_binance(timestamp, se_coin, se_quantity)), new_datas['Timestamp'], new_datas['Received Currency'], new_datas['Received Quantity'], new_datas['Sent Currency'], new_datas['Sent Quantity']))

new_datas['Total trade value in USDT'] = list(map(lambda trades, fees: float(trades)  + float(fees), new_datas['Trade value in USDT'], new_datas['Fees value in USDT']))

pandas.DataFrame(new_datas).to_csv('coinbase.csv')

print(pandas.DataFrame(new_datas))