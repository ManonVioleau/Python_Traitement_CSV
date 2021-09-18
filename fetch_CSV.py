import pandas


data1 = pandas.read_csv(r'/home/manon/Bureau/Adrien/Adrien2/csv_final/binance_trades_20210914.csv')
data2 = pandas.read_csv(r'/home/manon/Bureau/Adrien/Adrien2/csv_final/binance_transfert_FAUX_without_USDT_value_2017-12-20_2021-09-07.csv')
data3 = pandas.read_csv(r'/home/manon/Bureau/Adrien/Adrien2/binance_transfert_without_USDT_value_2021-02-05_2021-09-07.csv')

# data1 = data1.drop(columns=['Unnamed: 0'])
# data2 = data2.drop(columns=['Unnamed: 0'])
# print(data1)
# print(data2)
frames = [data1, data2, data3]
result = pandas.concat(frames).drop_duplicates(subset ="Timestamp")
result = result.sort_values(by=['Timestamp'])
print(result)
result.to_csv('binance_all_2017-07-14_2021-09-07.csv')