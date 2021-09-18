import pandas

new_df = pandas.read_csv(r'/home/manon/Bureau/Adrien/Adrien2/coinbase_all_02.csv')

print(new_df)

new_datas = {'Coin': [], 'Achat': [], 'Vente': [], 'Total': []}
new_datas2 = {}

for index, row in new_df.iterrows():
    if row['Type'] == 'Trade' or row['Type'] == 'Buy' or row['Type'] == 'Sell':
        if row['Received Currency'] != '0.0':
            asset = row['Received Currency']
            previous_quantity_in = 0
            previous_quantity_out = 0
            if row['Received Currency'] in new_datas2:        
                previous_quantity_in = new_datas2[asset]['Achat']
                previous_quantity_out = new_datas2[asset]['Vente']
            quantity_in = previous_quantity_in + row['Trade value in USDT']
            total = previous_quantity_out - quantity_in
            new_balance = {row['Received Currency']: {'Achat': quantity_in,'Vente': previous_quantity_out, 'Total': total}}
            new_datas2.update(new_balance)
        if row['Sent Currency'] != '0.0':
            asset = row['Sent Currency']
            previous_quantity_in = 0
            previous_quantity_out = 0
            if row['Sent Currency'] in new_datas2:        
                previous_quantity_in = new_datas2[asset]['Achat']
                previous_quantity_out = new_datas2[asset]['Vente']
            quantity_out = previous_quantity_out + row['Trade value in USDT']
            total = quantity_out - previous_quantity_in
            new_balance = {row['Sent Currency']: {'Achat': previous_quantity_in,'Vente': quantity_out, 'Total': total}}
            new_datas2.update(new_balance)

print(new_datas2)
# cbpro : {'ETH': {'Achat': 9183.1544288815, 'Vente': 10239.3142276732, 'Total': 1056.1597987916994}, 'EUR': {'Achat': 5343.0355899999995, 'Vente': 19481.913210209208, 'Total': 14138.877620209209}, 'BTC': {'Achat': 30771.311005876505, 'Vente': 38038.78032558999, 'Total': 7267.4693197134875}, 'XTZ': {'Achat': 1150.347401, 'Vente': 397.36323, 'Total': -752.9841710000001}, 'XRP': {'Achat': 161.334, 'Vente': 197.487, 'Total': 36.15299999999999}, 'LINK': {'Achat': 395.233618, 'Vente': 415.2705900000001, 'Total': 20.036972000000105}, 'USDC': {'Achat': 43547.8165532632, 'Vente': 21782.1040135488, 'Total': -21765.712539714397}}
