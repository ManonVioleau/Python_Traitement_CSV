import pandas

new_df = pandas.read_csv(r'/home/manon/Bureau/Adrien/Adrien2/csv_final/binance_all_FAUX_2017-07-14_2021-09-07.csv')

# get balance
balance = {}

for index, row in new_df.iterrows():
    if row['Received Currency'] != '0.0':
        asset = row['Received Currency']
        previous_quantity = 0
        if row['Received Currency'] in balance:        
            previous_quantity = balance[asset]
        quantity = previous_quantity + row['Received Quantity']
        new_balance = {row['Received Currency']: quantity}
        balance.update(new_balance)
    if row['Sent Currency'] != '0.0':
        asset = row['Sent Currency']
        previous_quantity = 0
        if row['Sent Currency'] in balance:        
            previous_quantity = balance[asset]
        quantity = previous_quantity - row['Sent Quantity']
        new_balance = {row['Sent Currency']: quantity}
        balance.update(new_balance)
    if row['Fee Currency'] != '0.0':
        asset = row['Fee Currency']
        previous_quantity = 0
        if row['Fee Currency'] in balance:        
            previous_quantity = balance[asset]
        quantity = previous_quantity - row['Fee Quantity']
        new_balance = {row['Fee Currency']: quantity}
        balance.update(new_balance)

print(balance)

#coinbase + coinbase_pro {'BTC': 0.45176466749999966, 'EUR': 0.00023022349959123112, 'ETH': 2.20600536, 'LTC': 0.0, 'XTZ': 9.999999974752427e-07, 'XRP': -1.1368683772161603e-13, 'LINK': -3.552713678800501e-15, 'USDC': 10964.810728027238}