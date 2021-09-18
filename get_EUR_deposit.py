import pandas

new_df = pandas.read_csv(r'/home/manon/Bureau/Adrien/Adrien2/csv_final/coinbase_pro_20210901.csv')
# print(new_df)
# get balance
balance = {'Timestamp': [], 'Date': []}
assets = []