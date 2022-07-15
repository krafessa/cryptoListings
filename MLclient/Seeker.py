''' a class to implement diverse fonctionnalities such as listing Names or prices...
     @author : KRAFESS AYYOUB
     @date : 15-07-2022
'''

from http import client
import grpc
import dataService_pb2, dataService_pb2_grpc
from datetime import datetime, timedelta

class Seeker(object):

    def __init__(self):
        self.port = 50051
        self.channel = grpc.insecure_channel('{}:{}'.format('localhost', self.port))
        self.stub = dataService_pb2_grpc.DATAStub(self.channel)

    def lookUpNames(self, fi):

        '''    
            return the names for the cryptos corresponding to the given filter
            @return : list of names
        '''
        
        request = dataService_pb2.NamesRequest(filterDS = fi)
        result = self.stub.getNames(request)
        if len(fi.numfilterDS) == 0 and len(fi.catfilterDS) == 0:
            return result.nameDS
        else:
            dict_aux = {}
            final_list = []
            l = list(result.nameDS)
            for name in result.nameDS:
                dict_aux[name] = (l).count(name)
            for key in dict_aux.keys():
                if dict_aux[key] == len(fi.catfilterDS) + len(fi.numfilterDS):
                    final_list.append(key)
            return final_list

    def lookUpStats(self, filter):
        '''
            return the static data for the cryptos corresponding to the given filter
            @return : list of dictionaries for each crypto, where the keys are properties (name, circulatingSupply, total
            SupplY, maxSupply, commits, forks, stars, watching, dateFirstListing)
        '''
        list_stat = []
        request = dataService_pb2.StatRequest(filterDS = filter)
        result = self.stub.getStat(request)
        size =len(result.nameDS)
        try : 
            for k in range(size):
                dict_stat = {}
                dict_stat['name'] = result.nameDS[k]
                cs =  result.circulatingSupplyDS[k].split(',')
                dict_stat['circulating supply'] = ''.join(cs)
                ts = result.totalSupplyDS[k].split(',')
                dict_stat['total supply'] = ''.join(ts)
                ms = result.maxSupplyDS[k].split(',')
                dict_stat['max supply'] = ''.join(ms)
                c = result.commitsDS[k].split('k')
                cc = c[0].split(',')
                try:
                    floatcc = float('.'.join(cc))*1000
                    dict_stat['commits'] = floatcc
                except ValueError:
                    dict_stat['commits'] = cc[0]
                f =result.forksDS[k].split('k')
                ff = f[0].split(',')
                try:
                    floatff = float('.'.join(ff))*1000
                    dict_stat['forks'] = floatff
                except ValueError:
                    dict_stat['forks'] = ff[0]
                s =result.starsDS[k].split('k')
                ss = s[0].split(',')
                try :
                    floatss = float('.'.join(ss))*1000
                    dict_stat['stars'] = floatss
                except ValueError:
                    dict_stat['stars'] = ss[0]
                w =result.watchingDS[k].split('k')
                ww = w[0].split(',')
                try:
                    floatww = float('.'.join(ww))*1000
                    dict_stat['watching'] = floatww
                except ValueError:
                    dict_stat['watching'] = ww[0]
                dict_stat['date first listing'] = result.dateFirstListingDS[k]
                list_stat.append(dict_stat)
        except IndexError:
            pass
        return list_stat

    def lookUpHists(self, filter, start = '', end = ''):
        ''''
            return the historical data for the cryptos corresponding to the given filter
            if start and end date parameters are not given, the program by default returns the historical prices for the 
            last month
            @return : dictionary of cryptos where the values are a dictionary as well  of  properties(name, open, close, high, low, volume, marketCap, date)
        '''
        if end == '' or end > datetime.now().strftime("%Y-%m-%d"):
            end = (datetime.now()- timedelta(days = 1)).strftime("%Y-%m-%d") 
        if start == '':
            start = (datetime.now() - timedelta(days = 30)).strftime("%Y-%m-%d")
        request = dataService_pb2.HistRequest(
            filterDS = filter,
            startDateDS = start,
            endDateDS = end,
        )
        result =  self.stub.getHist(request)
        return_dict = {}
        for name in result.nameDS:
            return_dict[name] = {}
        for k in range(len(list(result.nameDS))):
            aux_dict = return_dict[result.nameDS[k]]
            aux_dict['open'] = result.listOpenPrice[k].openPrice
            aux_dict['close'] = result.listClosePrice[k].closePrice
            aux_dict['high'] = result.listHighPrice[k].highPrice
            aux_dict['low'] = result.listLowPrice[k].lowPrice
            aux_dict['volume'] = result.volumeDS[k].volume
            aux_dict['marketcap'] = result.marketCapDS[k].marketCap
            aux_dict['date'] = result.dateDS[k].date
            return_dict[result.nameDS[k]] = aux_dict
        return return_dict

    def lookUpStat(self, named):
        ''' 
            return the static data for crypto wich the name is named
            @return : dictionary for properties of the crypto 
        '''
        request = dataService_pb2.StatDataRequest(name = named)
        result = self.stub.Stat(request)
        dict_stat = {}
        dict_stat['name'] = named
        cs =  result.circulatingSupplyDS.split(',')
        dict_stat['circulating supply'] = ''.join(cs)
        ts = result.totalSupplyDS.split(',')
        dict_stat['total supply'] = ''.join(ts)
        ms = result.maxSupplyDS.split(',')
        dict_stat['max supply'] = ''.join(ms)
        c = result.commitsDS.split('k')
        cc = c[0].split(',')
        try:
            floatcc = float('.'.join(cc))*1000
            dict_stat['commits'] = floatcc
        except ValueError:
            dict_stat['commits'] = cc[0]
        f =result.forksDS.split('k')
        ff = f[0].split(',')
        try:
            floatff = float('.'.join(ff))*1000
            dict_stat['forks'] = floatff
        except ValueError:
            dict_stat['forks'] = ff[0]
        s =result.starsDS.split('k')
        ss = s[0].split(',')
        try :
            floatss = float('.'.join(ss))*1000
            dict_stat['stars'] = floatss
        except ValueError:
            dict_stat['stars'] = ss[0]
        w =result.watchingDS.split('k')
        ww = w[0].split(',')
        try:
            floatww = float('.'.join(ww))*1000
            dict_stat['watching'] = floatww
        except ValueError:
            dict_stat['watching'] = ww[0]
        dict_stat['date first listing'] = result.dateFirstListingDS
        return dict_stat

    def lookUpPrice(self, named, start = '', end = ''):
        ''' 
            return the historical data for crypto wich the name is named
            @return : dictionary for historical prices of the crypto 
        '''
        if end == '' or end > datetime.now().strftime("%Y-%m-%d"):
            end = (datetime.now()- timedelta(days = 1)).strftime("%Y-%m-%d") 
        if start == '':
            start = (datetime.now() - timedelta(days = 30)).strftime("%Y-%m-%d")
        request = dataService_pb2.PriceDataRequest(
            nameDS = named,
            startDateDS = start,
            endDateDS = end,
        )
        list_price = []
        result = self.stub.Price(request)
        size = len(result.openDS)
        for k in range(size):
            dict_price = {}
            dict_price['open'] = result.openDS[k]
            dict_price['close'] = result.closeDS[k]
            dict_price['high'] = result.highDS[k]
            dict_price['low'] = result.lowDS[k]
            dict_price['volume'] = result.volumeDS[k]
            dict_price['marketCap'] = result.marketcapDS[k]
            dict_price['date'] = result.dateDS[k]
            list_price.append(dict_price)
        return list_price


    