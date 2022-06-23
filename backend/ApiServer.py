'''
     API server to process the request sent by the client and then accessed the database
     and returns an answer corresponding to the sent request 
     @author : KRAFESS AYYOUB
     @date : 23-06-2022
'''

from pymongo import MongoClient 
from concurrent import futures
import time
import logging
import json
import grpc
import api_pb2
import api_pb2_grpc
from datetime import datetime, timedelta
listProperties = ['volume', 'marketCap', 'open', 'high', 'low', 'close', 'circulatingSupply', 'totalSupply', 'maxSupply']
NumericalFilters = ['lt', 'gt']
def convert(str):
    '''
        converts a string price to a float price
        @param : the string representation of the price
        @return : float value of price
    '''
    if isinstance(str, float):
        return 0
    try:
        d = str.index(',')
        newStr = str[:d]+'.'+str[d+1:]
        l = newStr.split()
        return float(''.join(l[:-1]))
    except ValueError:
        l = str.split()
        return float(''.join(l[:-1]))

class API(api_pb2_grpc.APIServicer):

    def Connection(self):
        ''' 
            opens a connection to the mongoclient and gives access to the database
            @return : cryptos database
        '''
        client = MongoClient('localhost', 27017)
        db = client['Cryptos']
        return db

    def ListNames(self, request, context):
        '''
            returns a list of the crypto names corresponding to the given filter as
            a parameter
        '''
        logging.info('Received a listNames request from the data service !')
        filter = request.filterApi 
        numFilters,catFilters,properties,values = filter.numFilterApi,list(filter.tag),filter.propertiesApi,filter.valuesApi
        db = self.Connection()
        Stat1, Stat2, Stat3 = db['Static Data A->H'], db['Static Data I->P'],db['Static Data Q->Z']
        col = db['CoinMarketCap']
        Hist1, Hist2, Hist3 = col['Historical Prices A->H'], col['Historical Prices I->P'], col['Historical Prices Q->Z']
        listNames = []
        if len(catFilters) !=0:
            dic1,dic2,dic3 = Stat1.find(), Stat2.find(),Stat3.find()
            for doc1 in dic1:
                try :
                    for tag in doc1['tags']:
                        if tag in catFilters:
                            listNames.append(doc1['name'])
                except KeyError:
                    pass
            for doc2 in dic2:
                try :
                    for tag in doc2['tags']:
                        if tag in catFilters:
                            listNames.append(doc2['name'])
                except KeyError:
                    pass
            for doc3 in dic3:
                try :
                    for tag in doc3['tags']:
                        if tag in catFilters:
                            listNames.append(doc3['name'])
                except KeyError:
                    pass     
        else:
            dic1,dic2,dic3 = Stat1.find(), Stat2.find(),Stat3.find()
            for doc1 in dic1:
                listNames.append(doc1['name'])
            for doc2 in dic2:
                listNames.append(doc2['name'])
            for doc3 in dic3:
                listNames.append(doc3['name'])
        aux_list = []
        if len(numFilters)!=0:
            for (property, numfilter, value) in zip(properties, numFilters,values):
                if len(listNames) !=0:
                    for filteredname in listNames:
                        if ord(filteredname[0]) <= ord('H') and ord(filteredname[0]) >= ord('A'):
                            hist_prices = Hist1.find_one({'name' : filteredname})['Hist']
                        elif ord(filteredname[0]) <= ord('P') and ord(filteredname[0]) >= ord('I'):
                            hist_prices = Hist2.find_one({'name' : filteredname})['Hist']
                        else:
                            hist_prices = Hist3.find_one({'name' : filteredname})['Hist']
                        wanted_date = datetime.strftime(datetime.now() - timedelta(days  = 1), "%Y-%m-%d")
                        wanted_date = datetime.strptime(wanted_date, "%Y-%m-%d")
                        for k in range(1, len(hist_prices)):
                            if datetime.strptime(hist_prices[k]['timeOpen'][:10], "%Y-%m-%d") <= wanted_date and datetime.strptime(hist_prices[k]['timeClose'][:10],"%Y-%m-%d")>=wanted_date:
                                hist = hist_prices[k]
                                break
                        if numfilter==0:
                            if hist['quote'][listProperties[property]] >value:
                                pass
                            else:
                                aux_list.append(filteredname)
                        else:
                            if hist['quote'][listProperties[property]] < value:
                                pass
                            else:
                                aux_list.append(filteredname)
                else:
                    break
            n = len(numFilters)
            final_list = {}
            for name in aux_list:
                if aux_list.count(name) == n:
                    final_list[name] = ''
            logging.info('Answered corectly the data service with a list of names ...')
            return api_pb2.ListNamesResponse(name = final_list.keys()) 
        else:
            logging.info('Answered corectly the data service with a list of names ...')
            return api_pb2.ListNamesResponse(name = listNames) 

    def ListStat(self, request, context):
        '''
            returns a list of the static data of cryptos corresponding to the given filter as
            a parameter
        '''
        logging.info('Received a listStat request from the data service !')
        filter = request.FilterApi 
        numFilters,catFilters,properties,values = filter.numFilterApi,list(filter.tag),filter.propertiesApi,filter.valuesApi
        db = self.Connection()
        Stat1, Stat2, Stat3 = db['Static Data A->H'], db['Static Data I->P'],db['Static Data Q->Z']
        col = db['CoinMarketCap']
        Hist1, Hist2, Hist3 = col['Historical Prices A->H'], col['Historical Prices I->P'], col['Historical Prices Q->Z']
        listNames = []
        if len(catFilters) !=0:
            dic1,dic2,dic3 = Stat1.find(), Stat2.find(),Stat3.find()
            for doc1 in dic1:
                try :
                    for tag in doc1['tags']:
                        if tag in catFilters:
                            listNames.append(doc1['name'])
                except KeyError:
                    pass
            for doc2 in dic2:
                try :
                    for tag in doc2['tags']:
                        if tag in catFilters:
                            listNames.append(doc2['name'])
                except KeyError:
                    pass
            for doc3 in dic3:
                try :
                    for tag in doc3['tags']:
                        if tag in catFilters:
                            listNames.append(doc3['name'])
                except KeyError:
                    pass    
        else:
            dic1,dic2,dic3 = Stat1.find(), Stat2.find(),Stat3.find()
            for doc1 in dic1:
                listNames.append(doc1['name'])
            for doc2 in dic2:
                listNames.append(doc2['name'])
            for doc3 in dic3:
                listNames.append(doc3['name'])
        finalList = []
        if len(numFilters)!=0:
            for (property, numfilter, value) in zip(properties, numFilters,values):
                if len(listNames) !=0:
                    for filteredname in listNames:
                        if ord(filteredname[0]) <= ord('H') and ord(filteredname[0]) >= ord('A'):
                            hist_prices = Hist1.find_one({'name' : filteredname})['Hist']
                        elif ord(filteredname[0]) <= ord('P') and ord(filteredname[0]) >= ord('I'):
                            hist_prices = Hist2.find_one({'name' : filteredname})['Hist']
                        else:
                            hist_prices = Hist3.find_one({'name' : filteredname})['Hist']
                        wanted_date = datetime.strftime(datetime.now() - timedelta(days  = 1), "%Y-%m-%d")
                        wanted_date = datetime.strptime(wanted_date, "%Y-%m-%d")
                        for k in range(1, len(hist_prices)):
                            if datetime.strptime(hist_prices[k]['timeOpen'][:10], "%Y-%m-%d") <= wanted_date and datetime.strptime(hist_prices[k]['timeClose'][:10],"%Y-%m-%d")>=wanted_date:
                                hist = hist_prices[k]
                                break
                        if numfilter==0:
                            if hist['quote'][listProperties[property]] >value:
                                pass
                            else:
                                finalList.append(filteredname)
                        else:
                            if hist['quote'][listProperties[property]] < value:
                                pass
                            else:
                                finalList.append(filteredname)
                else:
                    break
            n = len(numFilters)
            final_list = {}
            for name in finalList:
                if finalList.count(name) == n:
                    final_list[name] = ''
            finalList = final_list.keys()
        else:
            finalList = listNames
        nameApis, dateFirstListingApis, commitsApis, forksApis, starsApis, watchingApis, circulatingSupplyApis, totalSupplyApis, maxSupplyApis = [],[],[],[],[],[],[],[],[]
        for Name in finalList:
            if ord(Name[0]) <= ord('H') and ord(Name[0]) >= ord('A'):
                query = Stat1.find_one({'name' : Name})
            elif ord(Name[0]) <= ord('P') and ord(Name[0]) >= ord('I'):
                query = Stat2.find_one({'name' : Name})
            else:
                query = Stat3.find_one({'name' : Name})
            try :
                    commitsApis.append(query['Commits'])
                    forksApis.append(query['Forks'])
                    starsApis.append(query['Stars'])
                    watchingApis.append(query['Watching'])
                    nameApis.append(Name)
                    dateFirstListingApis.append(query['firstListing'])
                    circulatingSupplyApis.append(query['circulatingSupply'])
                    totalSupplyApis.append(query['totalSupply'])
                    maxSupplyApis.append(query['maxSupply']) 
            except KeyError :
                    nameApis.append(Name)
                    dateFirstListingApis.append(query['firstListing'])
                    circulatingSupplyApis.append(query['circulatingSupply'])
                    totalSupplyApis.append(query['totalSupply'])
                    maxSupplyApis.append(query['maxSupply'])
                    commitsApis.append('--')
                    forksApis.append('--')
                    starsApis.append('--')
                    watchingApis.append('--')
        logging.info("Answered correctly the dataservice with the list of static data ...")
        res = api_pb2.ListStatResponse(
            nameApi = nameApis,
            dateFirstListingApi = dateFirstListingApis,
            commitsApi = commitsApis,
            forksApi = forksApis,
            startsApi = starsApis,
            watchingApi = watchingApis,
            circulatingSupplyApi = circulatingSupplyApis,
            totalSupplyApi = totalSupplyApis,
            maxSupplyApi = maxSupplyApis,
        )
        return res

    def ListHist(self, request, context):
        '''
            returns a list of historical data of  the cryptos corresponding to the given filter as
            a parameter
        '''
        logging.info('Received a ListHist request from the data service !')
        filter = request.filterApi 
        numFilters,catFilters,properties,values = filter.numFilterApi,list(filter.tag),filter.propertiesApi,filter.valuesApi
        db = self.Connection()
        Stat1, Stat2, Stat3 = db['Static Data A->H'], db['Static Data I->P'],db['Static Data Q->Z']
        col = db['CoinMarketCap']
        Hist1, Hist2, Hist3 = col['Historical Prices A->H'], col['Historical Prices I->P'], col['Historical Prices Q->Z']
        listNames = []
        if len(catFilters) !=0:
            dic1,dic2,dic3 = Stat1.find(), Stat2.find(),Stat3.find()
            for doc1 in dic1:
                try :
                    for tag in doc1['tags']:
                        if tag in catFilters:
                            listNames.append(doc1['name'])
                except KeyError:
                    pass
            for doc2 in dic2:
                try :
                    for tag in doc2['tags']:
                        if tag in catFilters:
                            listNames.append(doc2['name'])
                except KeyError:
                    pass
            for doc3 in dic3:
                try :
                    for tag in doc3['tags']:
                        if tag in catFilters:
                            listNames.append(doc3['name'])
                except KeyError:
                    pass    
        else:
            dic1,dic2,dic3 = Stat1.find(), Stat2.find(),Stat3.find()
            for doc1 in dic1:
                listNames.append(doc1['name'])
            for doc2 in dic2:
                listNames.append(doc2['name'])
            for doc3 in dic3:
                listNames.append(doc3['name'])
        finalList = []
        if len(numFilters)!=0:
            for (property, numfilter, value) in zip(properties, numFilters,values):
                if len(listNames) !=0:
                    for filteredname in listNames:
                        if ord(filteredname[0]) <= ord('H') and ord(filteredname[0]) >= ord('A'):
                            hist_prices = Hist1.find_one({'name' : filteredname})['Hist']
                        elif ord(filteredname[0]) <= ord('P') and ord(filteredname[0]) >= ord('I'):
                            hist_prices = Hist2.find_one({'name' : filteredname})['Hist']
                        else:
                            hist_prices = Hist3.find_one({'name' : filteredname})['Hist']
                        wanted_date = datetime.strftime(datetime.now() - timedelta(days  = 1), "%Y-%m-%d")
                        wanted_date = datetime.strptime(wanted_date, "%Y-%m-%d")
                        for k in range(1, len(hist_prices)):
                            if datetime.strptime(hist_prices[k]['timeOpen'][:10], "%Y-%m-%d") <= wanted_date and datetime.strptime(hist_prices[k]['timeClose'][:10],"%Y-%m-%d")>=wanted_date:
                                hist = hist_prices[k]
                                break
                        if numfilter==0:
                            if hist['quote'][listProperties[property]] >value:
                                pass
                            else:
                                finalList.append(filteredname)
                        else:
                            if hist['quote'][listProperties[property]] < value:
                                pass
                            else:
                                finalList.append(filteredname)
                else:
                    break
            n = len(numFilters)
            final_list = {}
            for name in finalList:
                if finalList.count(name) == n:
                    final_list[name] = ''
            finalList = final_list.keys()
        else:
            finalList = listNames
        startDate, endDate = datetime.strptime(request.startDateApi, '%Y-%m-%d'),datetime.strptime(request.endDateApi, '%Y-%m-%d')
        NameAPI,OpenAPI, CloseAPI, HighAPI, LowAPI, VolumeAPI, MarketCapAPI, DateAPI = [],[],[],[],[],[],[],[]
        for filteredName in finalList:
            if ord(filteredName[0]) >= ord('A') and ord(filteredName[0]) <= ord('H'):
                hist = Hist1.find_one({'name' : filteredName})['Hist'][:-4]
            elif ord(filteredName[0]) >= ord('I') and ord(filteredName[0]) <= ord('P'):
                hist = Hist2.find_one({'name' : filteredName})['Hist'][:-4]
            else:
                hist = Hist3.find_one({'name' : filteredName})['Hist'][:-4]
            for date in hist[1:]:
                currentDate = datetime.strptime(date['quote']['timestamp'][:10], '%Y-%m-%d')
                if currentDate >= startDate and currentDate <= endDate:
                        NameAPI.append(filteredName)
                        OpenAPI.append(date['quote']['open'])
                        CloseAPI.append(date['quote']['close'])
                        HighAPI.append(date['quote']['high'])
                        LowAPI.append(date['quote']['low'])
                        VolumeAPI.append(date['quote']['volume'])
                        MarketCapAPI.append(date['quote']['marketCap'])
                        DateAPI.append(date['quote']['timestamp'])
        logging.info('Answered correctly the data service with the list of historical prices ...')
        return api_pb2.ListHistResponse(nameApi = NameAPI, openApi = OpenAPI, closeApi = CloseAPI, highApi = HighAPI, lowApi = LowAPI, volumeApi = VolumeAPI, marketCapApi = MarketCapAPI, dateApi = DateAPI)
        
    def getPrice(self, request, context):
        '''
            returns a list of the historical prices for the crypto name given as
            a parameter
        '''
        logging.info('GetPrice request made !')
        db = self.Connection()
        col = db['CoinMarketCap']
        Hist1, Hist2, Hist3 = col['Historical Prices A->H'], col['Historical Prices I->P'], col['Historical Prices Q->Z']
        startDate, endDate =datetime.strptime(request.startDateGHist[:10], '%Y-%m-%d'),datetime.strptime(request.endDateHist[:10], '%Y-%m-%d')
        name = request.name
        listOpen, listClose, listHigh, listLow,listDate, listVolume, listMarketCap = [],[],[],[],[],[],[]
        if ord(name[0]) >= ord('A') and ord(name[0]) <= ord('H'):
            hist = Hist1.find_one({'name' : name})
        elif ord(name[0]) >= ord('I') and ord(name[0]) <= ord('P'):
            hist = Hist2.find_one({'name' : name})
        else:
            hist = Hist3.find_one({'name' : name})
        try:
            hist2 = hist['Hist']
            hist3 = hist2[1:]
            elt = hist3[0]
            last_date = datetime.strptime(elt['quote']['timestamp'][:10], '%Y-%m-%d')
            number_of_days_end_start = (endDate - startDate).days
            number_of_days_last_start = (last_date - startDate).days
            number_of_days_last_end = (last_date-endDate).days
            difference2 = last_date - endDate
            index_end_occurence = difference2.days
            index_start_occurence = last_date - startDate
            if number_of_days_last_end <0 and  number_of_days_last_start<0:
                logging.info('some of the data corresponding to days in between '+request.endDateHist+' and '+request.startDateGHist+' is  not found in the data base, please upload your data base!!')
            elif number_of_days_last_end <0 and number_of_days_last_start >=0:
                logging.info('some of the data corresponding to days in between '+elt['quote']['timestamp']+' and '+request.endDateHist+' is  not found in the data base, please upload your data base!!') 
                for quote in hist3[0:number_of_days_last_start]:
                    listOpen.append(quote['quote']['open'])
                    listClose.append(quote['quote']['close'])
                    listHigh.append(quote['quote']['high'])
                    listLow.append(quote['quote']['low'])
                    listMarketCap.append(quote['quote']['marketCap'])
                    listVolume.append(quote['quote']['volume'])
                    listDate.append(quote['quote']['timestamp'])
            else:
                for quote in hist3[index_end_occurence:index_end_occurence+number_of_days_end_start]:
                    listOpen.append(quote['quote']['open'])
                    listClose.append(quote['quote']['close'])
                    listHigh.append(quote['quote']['high'])
                    listLow.append(quote['quote']['low'])
                    listMarketCap.append(quote['quote']['marketCap'])
                    listVolume.append(quote['quote']['volume'])
                    listDate.append(quote['quote']['timestamp'])
        except IndexError:
            pass
        return api_pb2.PriceResponse(
            openPrice = listOpen,
            closePrice = listClose,
            highPrice = listHigh,
            lowPrice = listLow,
            volume = listVolume,
            marketCap = listMarketCap,
            datePrice = listDate,
        )
        
    def getStaticData(self, request, context):
        '''
            returns a list of the static data for the crypto name given as
            a parameter
        '''
        logging.info('GetStaticData request made !')
        db = self.Connection()
        Stat1, Stat2, Stat3 = db['Static Data A->H'], db['Static Data I->P'],db['Static Data Q->Z']
        name = request.name
        if ord(name[0]) >= ord('A') and ord(name[0]) <= ord('H'):
            stat = Stat1.find_one({'name' : name})
        elif ord(name[0]) >= ord('I') and ord(name[0]) <= ord('P'):
            stat = Stat2.find_one({'name' : name})
        else:
            stat = Stat3.find_one({'name' : name})    
        try : 
            logging.info('Answered correctly the dataservice with static data for the given crypto...')
            return api_pb2.StaticDataResponse(
                circulatingSupply = stat['circulatingSupply'],
                totalSupply = stat['totalSupply'],
                maxSupply = stat['maxSupply'],
                dateFirstListing = stat['firstListing'],
                commits = stat['Commits'],
                forks = stat['Forks'],
                stars = stat['Stars'],
                watching = stat['Watching'],
            )
        except KeyError:
            logging.info('Answered correctly the dataservice with static data for the given crypto...')
            return api_pb2.StaticDataResponse(
                circulatingSupply = stat['circulatingSupply'],
                totalSupply = stat['totalSupply'],
                maxSupply = stat['maxSupply'],
                dateFirstListing = stat['firstListing'],
                commits = '--',
                forks = '--',
                stars = '--',
                watching = '--',
            )              
                 
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    api_pb2_grpc.add_APIServicer_to_server(API(), server)
    port = 9999
    server.add_insecure_port(f'[::]:{port}')
    try: 
        server.start()
        logging.info('server ready on port %r', port)
        server.wait_for_termination()
    except KeyboardInterrupt:
        logging.info('server closed due to KeyboardInterrupt !')




           



                
                

            
