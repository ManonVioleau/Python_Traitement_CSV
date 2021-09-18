import config
import pandas
from urllib.request import urlopen
import json
from binance.client import Client
from binance.exceptions import BinanceAPIException

client_binance = Client(config.BINANCE_API_KEY, config.BINANCE_API_SECRET)

data = pandas.read_excel(r'/home/manon/Bureau/Adrien/Adrien2/csv/coinbasepro_20190226_20210105.ods', engine="odf")
print(data)


new_datas = {'Portfolio' : [], 'Type' : [], 'Date' : [], 'Received Currency' : [], 'Received Quantity' : [], 'Sent Currency' : [], 'Sent Quantity' : [], 'Fee Currency' : [], 'Fee Quantity' : [], 'Trade value in USDT' : [], 'Fees value in USDT' : [], 'Total trade value in USDT' : [], 'Timestamp' : []}
row_prec = {'ID': '00'}
new_row = {'Portfolio' : '', 'Type' : '', 'Date' : '', 'Received Currency' : '', 'Received Quantity' : 0, 'Sent Currency' : '', 'Sent Quantity' : 0, 'Fee Currency' : '', 'Fee Quantity' : 0, 'Trade value in USDT' : '', 'Fees value in USDT' : '', 'Total trade value in USDT' : '', 'Timestamp' : ''}
hasbegin = False

for index, row in data.iterrows():
    if row['Type'] == 'Deposit':
        new_datas['Portfolio'].append('Coinbase Pro')
        date = str(str(row['Time']).split(' ')[0]) + ' ' + str(str(row['Time']).split(' ')[1])
        new_datas['Date'].append(date)
        new_datas['Timestamp'].append((pandas.to_datetime(date) - pandas.Timestamp("1970-01-01 00:00:00")) // pandas.Timedelta('1ms'))
        new_datas['Fees value in USDT'].append(0)
        new_datas['Trade value in USDT'].append(0)
        new_datas['Total trade value in USDT'].append(0)
        # 
        new_datas['Type'].append('Deposit')
        new_datas['Received Currency'].append(str(row['Amount']).split(' ')[1])
        new_datas['Received Quantity'].append(float(str(row['Amount']).split(' ')[0]))
        new_datas['Sent Currency'].append(0.0)
        new_datas['Sent Quantity'].append(0.0)
        new_datas['Fee Currency'].append(0.0)
        new_datas['Fee Quantity'].append(0.0)
    if row['Type'] == 'Withdrawal':
        new_datas['Portfolio'].append('Coinbase Pro')
        date = str(str(row['Time']).split(' ')[0]) + ' ' + str(str(row['Time']).split(' ')[1])
        new_datas['Date'].append(date)
        new_datas['Timestamp'].append((pandas.to_datetime(date) - pandas.Timestamp("1970-01-01 00:00:00")) // pandas.Timedelta('1ms'))
        new_datas['Fees value in USDT'].append(0)
        new_datas['Trade value in USDT'].append(0)
        new_datas['Total trade value in USDT'].append(0)
        # 
        new_datas['Type'].append('Withdrawal')
        new_datas['Received Currency'].append(0.0)
        new_datas['Received Quantity'].append(0.0)
        new_datas['Sent Currency'].append(str(row['Amount']).split(' ')[1])
        new_datas['Sent Quantity'].append(-1 * float(str(row['Amount']).split(' ')[0]))
        new_datas['Fee Currency'].append(0.0)
        new_datas['Fee Quantity'].append(0.0)
    if row['Type'] == 'Match':
        if str(row_prec['ID'])[:-1] != str(row['ID'])[:-1]:   
            hasbegin = False     
            # Réinitialisation de new_rom
            new_row = {'Portfolio' : '', 'Type' : '', 'Date' : '', 'Received Currency' : '', 'Received Quantity' : 0, 'Sent Currency' : '', 'Sent Quantity' : 0, 'Fee Currency' : '', 'Fee Quantity' : 0, 'Trade value in USDT' : '', 'Fees value in USDT' : '', 'Total trade value in USDT' : '', 'Timestamp' : ''}        
            new_row['Type'] = 'Trade'
            row_prec['ID'] = row['ID']
            if float(str(row['Amount']).split(' ')[0]) > 0:
                new_row['Received Currency'] = str(row['Amount']).split(' ')[1]
                new_row['Received Quantity'] = float(str(row['Amount']).split(' ')[0])
            else:
                new_row['Sent Currency'] = str(row['Amount']).split(' ')[1]
                new_row['Sent Quantity'] = -1 * float(str(row['Amount']).split(' ')[0])
        else:
            if float(str(row['Amount']).split(' ')[0]) > 0:
                new_row['Received Currency'] = str(row['Amount']).split(' ')[1]
                new_row['Received Quantity'] = float(str(row['Amount']).split(' ')[0])
            else:
                new_row['Sent Currency'] = str(row['Amount']).split(' ')[1]
                new_row['Sent Quantity'] = -1 * float(str(row['Amount']).split(' ')[0])
            # remplissage
            if hasbegin == False:
                new_datas['Portfolio'].append('Coinbase Pro')
                date = str(str(row['Time']).split(' ')[0]) + ' ' + str(str(row['Time']).split(' ')[1])
                new_datas['Date'].append(date)
                new_datas['Timestamp'].append((pandas.to_datetime(date) - pandas.Timestamp("1970-01-01 00:00:00")) // pandas.Timedelta('1ms'))
                new_datas['Fees value in USDT'].append(0)
                new_datas['Trade value in USDT'].append(0)
                new_datas['Total trade value in USDT'].append(0)
                # 
                new_datas['Type'].append(new_row['Type'])
                new_datas['Received Currency'].append(new_row['Received Currency'])
                new_datas['Received Quantity'].append(new_row['Received Quantity'])
                new_datas['Sent Currency'].append(new_row['Sent Currency'])
                new_datas['Sent Quantity'].append(new_row['Sent Quantity'])
                new_datas['Fee Currency'].append(0.0)
                new_datas['Fee Quantity'].append(0.0)
                hasbegin = True
            else:
                if float(str(row['Amount']).split(' ')[0]) > 0:
                    new_datas['Received Quantity'][-1] += float(str(row['Amount']).split(' ')[0])
                else:
                    new_datas['Sent Quantity'][-1] += -1 * float(str(row['Amount']).split(' ')[0])
    if row['Type'] == 'Fee':
        new_row['Fee Currency'] = str(row['Amount']).split(' ')[1]
        new_row['Fee Quantity'] = -1 * float(str(row['Amount']).split(' ')[0])
        # remplissage de new_data
        new_datas['Fee Currency'][-1] = new_row['Fee Currency']
        new_datas['Fee Quantity'][-1] += new_row['Fee Quantity']

new_df = pandas.DataFrame(new_datas)
trades = []
fees = []
total = []

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

def get_value_in_USD(date, quantity):
    date = str(date).split(' ')[0]
    access_key = config.EXCHANGERATESAPI_API_KEY
    url = 'http://api.exchangeratesapi.io/v1/' + date + '?access_key=' + access_key + '&base=EUR&symbols=USD'
    
    response = urlopen(url)

    data_json = json.loads(response.read())

    result = float(data_json['rates']['USD']) * float(quantity)

    return result

for index, row in new_df.iterrows():
    if row['Type'] == 'Deposit':
        if row['Received Currency'] != 'EUR':
            trade = get_value_in_USDT_width_binance(row['Timestamp'], row['Received Currency'], row['Received Quantity'])
        else:
            trade = get_value_in_USD(row['Date'], row['Received Quantity'])
    elif row['Type'] == 'Withdrawal':
        if row['Sent Currency'] != 'EUR':
            trade = get_value_in_USDT_width_binance(row['Timestamp'], row['Sent Currency'], row['Sent Quantity'])
        else:
            trade = get_value_in_USD(row['Date'], row['Sent Quantity'])
    else:
        if row['Received Currency'] != 'EUR' and row['Received Currency'] != 'XTZ':
            trade = get_value_in_USDT_width_binance(row['Timestamp'], row['Received Currency'], row['Received Quantity'])
        else:
            trade = get_value_in_USDT_width_binance(row['Timestamp'], row['Sent Currency'], row['Sent Quantity'])
    if row['Fee Currency'] != 0:
        if row['Fee Currency'] != 'EUR':
            fee = get_value_in_USDT_width_binance(row['Timestamp'], row['Fee Currency'], row['Fee Quantity'])
        else:
            fee = get_value_in_USD(row['Date'], row['Fee Quantity'])
    else:
        fee = 0
    trades.append(trade)
    fees.append(fee)
    total.append(trade + fee)


new_df['Trade value in USDT'] = trades
new_df['Fees value in USDT'] = fees
new_df['Total trade value in USDT'] = total

new_df.to_csv('coinbase_pro_01.csv')

print(new_df)