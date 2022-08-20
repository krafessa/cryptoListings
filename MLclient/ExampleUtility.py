import Seeker
import pandas as pd
import statsInputCompute as st
from random import randint
import csv

''' If wanted; create a filter for the data'''
seeker = Seeker.Seeker()
properties = [Seeker.dataService_pb2.openDS, Seeker.dataService_pb2.closeDS]
numconditions = [Seeker.dataService_pb2.gtDS, Seeker.dataService_pb2.gtDS]
values = [10000, 20000]
tags = [Seeker.dataService_pb2.CategoricalFilters(tagDS = 'Cryptocurrency')]
filter = Seeker.dataService_pb2.Filter(
    numfilterDS = [],
    catfilterDS = [],
    propertiesDS = [] ,
    valuesDS = []
)

'''getting all the historical data for all cryptos in a year '''
all_data = seeker.lookUpHists(filter, '2021-08-01', '2022-08-01')
col = ['name', 'min_open','min_volume','min_marketcap','max_open', 'max_volume', 'max_marketcap', 'mean_open', 'mean_volume', 'mean_marketcap', 'var_open', 'var_volume', 'var_marketcap','ratio_sharpe', 'listed' ]
df = pd.DataFrame(columns =col )
for name in all_data.keys():
    if len(all_data[name]['open']) == 0:
        pass
    else:
        aux_dict = {}
        aux_dict['name'] = name
        aux_dict['min_open'] = st.minValue(all_data[name]['open'])
        aux_dict['min_volume']= st.minValue(all_data[name]['volume'])
        aux_dict['min_marketcap'] = st.minValue(all_data[name]['marketcap'])
        aux_dict['max_open'] = st.maxValue(all_data[name]['open'])
        aux_dict['max_volume'] = st.maxValue(all_data[name]['volume'])
        aux_dict['max_marketcap'] = st.maxValue(all_data[name]['marketcap'])
        aux_dict['mean_open'] = st.meanValue(all_data[name]['open'])
        aux_dict['mean_volume'] = st.meanValue(all_data[name]['volume'])
        aux_dict['mean_marketcap'] = st.meanValue(all_data[name]['marketcap'])
        aux_dict['var_open'] = st.varianceValue(all_data[name]['open'])
        aux_dict['var_volume'] = st.varianceValue(all_data[name]['volume'])
        aux_dict['var_marketcap'] = st.varianceValue(all_data[name]['marketcap'])
        aux_dict['ratio_sharpe'] = st.sharpeRatioValue( all_data[name]['open'], 0.05)
        aux_dict ['listed'] = randint(0,1)
        df = df.append(aux_dict, ignore_index=True)
        
df.to_csv('list_data.csv', index=False) 

''' tests the function to get static data with only a fiter provided'''
#print(seeker.lookUpStats(filter))

''' tests the function to get static data with the name of the crypto provided'''
#print(seeker.lookUpStat('AVALANCHE'))

''' tests the function to get historical data with a given filter'''
#print(seeker.lookUpHists(filter, '2021-08-01', '2022-08-01'))

''' tests the function to get historical data for a given crypto'''
#print(seeker.lookUpPrice('BITCOIN'))
#print(seeker.lookUpPrice('BITCOIN', '2022-08-06', '2022-08-12')) #first use
#print(seeker.lookUpPrice('BITCOIN', '2022-08-01', '2022-08-05')) #case 5
#print(seeker.lookUpPrice('BITCOIN', '2022-07-10', '2022-08-12')) #case 2
#print(seeker.lookUpPrice('BITCOIN', '2022-08-01', '2022-08-15')) #case 3
#print(seeker.lookUpPrice('BITCOIN', '2022-07-01', '2022-08-14')) #case 6
#print(seeker.lookUpPrice('BITCOIN', '2022-04-01', '2022-04-10')) #case 5
