from functions import get_value_in_USDT_width_binance
import pandas

new_df = pandas.read_csv(r'/home/manon/Bureau/Adrien/Adrien2/csv_final/binance_all_FAUX_2017-07-14_2021-09-07.csv')
# print(new_df)
# get balance
balance = {'Timestamp': [], 'Date': []}
assets = []
values = {}

for index, row in new_df.iterrows():
    re_cur = row['Received Currency']
    se_cur = row['Sent Currency']
    fee_cur = row['Fee Currency']
    if re_cur not in assets and re_cur != '0.0':
        assets.append(re_cur)
        values[re_cur] = []
    if se_cur not in assets and se_cur != '0.0':
        assets.append(se_cur)
        values[se_cur] = []
    if fee_cur not in assets and fee_cur != '0.0':
        assets.append(fee_cur)
        values[fee_cur] = []


for index, row in new_df.iterrows():
    balance['Timestamp'].append(row['Timestamp'])
    balance['Date'].append(row['Date'])
    for asset in assets:
        if asset in balance:
            balance[asset].append(balance[asset][-1])
            if asset != 'EUR':
                balance[str(asset) + ' value in USDT'].append(balance[str(asset) + ' value in USDT'][-1])
        else:
            balance[asset] = [0]
            if asset != 'EUR':
                balance[str(asset) + ' value in USDT'] = [0]
    if row['Received Currency'] != '0.0' and row['Received Currency'] != 'EUR':
        asset = row['Received Currency']
        balance[asset][-1] = float(balance[asset][-1] + row['Received Quantity'])
    if row['Sent Currency'] != '0.0' and row['Sent Currency'] != 'EUR':
        asset = row['Sent Currency']
        if len(str(row['Sent Quantity']).split('E')) > 1:
            row['Sent Quantity'] = str(row['Sent Quantity']).split('E')[0].replace(',', '.')
        balance[asset][-1] = float(balance[asset][-1] - float(row['Sent Quantity']))
    if row['Fee Currency'] != '0.0' and row['Fee Currency'] != 'EUR':
        asset = row['Fee Currency']
        balance[asset][-1] = float(balance[asset][-1] - row['Fee Quantity'])
    if row['Type'] == 'Deposit' and row['Received Currency'] == 'EUR':
        balance['EUR'][-1] = float(balance['EUR'][-1] + row['Received Quantity'])


new_balance = pandas.DataFrame(balance)
print(values)
for index, row in new_balance.iterrows():
    for asset in assets:
        if asset not in ('EUR', '', '0.0', 0):
            if float(row[asset]) != 0:
                # print(row['Timestamp'], asset, row[asset], row[asset + ' value in USDT'])
                value = get_value_in_USDT_width_binance(row['Timestamp'], asset, row[asset])
                values[asset].append(value)
            else:
                values[asset].append(0)
    
for asset in assets:
    if asset not in ('EUR', '', '0.0', 0):
        new_balance[asset + ' value in USDT'] = values[asset]

print(new_balance)


new_balance.to_csv('binance_balance_time.csv')

