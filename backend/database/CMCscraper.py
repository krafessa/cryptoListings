'''
     scraps the CoinMarketCap website, and stores the data extracted in 
     the appropriate collection for the Cryptos Database
     @author : KRAFESS AYYOUB
     @date : 20-06-2022
'''
from functools import lru_cache
import json
import logging
import time
from pandas import Series
import sys
from multiprocessing import Pool
import dbconnect

from urllib.request import Request, urlopen

CMC = dbconnect.connect()['CoinMarketCap']
data = dbconnect.pd.read_pickle(r'src\stage\backend\database\Names&Symbols.pkl')

def getCryptoNames(numberOfCryptos):
     ''' 
          Returns the first 'numberOfCryptos' names of the cryptos in the CoinMarketCap
          in a descendant ordrer by the market cap ranking
          @return : list of names (list of strings)
     '''
     list_names = []
     list_of_all_names = Series.to_list(data['name'])
     for page in range(numberOfCryptos//100):
          urlCMC = "https://coinmarketcap.com/?page="+str(page)
          header = {
               'User-Agent' : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:103.0) Gecko/20100101 Firefox/103.0"
          }
          response = dbconnect.requests.get(urlCMC, headers=header)
          soup = dbconnect.BeautifulSoup(response.content, 'lxml')
          table = soup.find('table', class_ = "h7vnx2-2 czTsgW cmc-table")
          table_body = table.find('tbody')
          for row in table_body.find_all('tr'):
               columns= row.find_all('td')
               if columns != []:
                    name = columns[2].text
                    for elt in list_of_all_names:
                         aux_list_name = name.split(' ')
                         aux_list_elt = elt.split(' ')
                         if elt in name and len(aux_list_elt) == len(aux_list_name):
                              list_names.append(elt)
                              break
     return list_names

def getDayStatistics(cryptoName):
     ''' 
          collects the daily statistics for a given crypto
          @param: cryptoName is the name of the crypto that we want to scrap
          @param: CMC is the name of the collection where to insert data
     '''
     list_data = ["--","--","--","--","--","--","--","--"]
     try:
          dictH = {}
          dictS = {}
          slug = Series.to_list(data[data['name'] == cryptoName]['slug'])[0]
          urlCMC = "https://coinmarketcap.com/currencies/"+slug+"/"
          header = {
               'User-Agent' : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:103.0) Gecko/20100101 Firefox/103.0"
          }
          responseCrypto = dbconnect.requests.get(urlCMC, headers= header)
          soup = dbconnect.BeautifulSoup(responseCrypto.content, 'html.parser')
          ''' price data'''
          dictH['currentPrice'] = soup.find_all('div', class_ = 'priceValue')[0].text
          LHPrice = soup.find_all('span', class_ = 'n78udj-5 dBJPYV')
          dictH['lowestPrice'], dictH['highestPrice'] = LHPrice[0].text, LHPrice[1].text
          Market = soup.find_all('div', class_ = 'statsValue')
          dictH['marketCap'],dictH['volume'] = Market[0].text,Market[2].text
          dictH['date'] = dbconnect.datetime.now()
          dbconnect.setHist(cryptoName, dictH, CMC, 'CMC',True)
          dictS['firstListing'] = Series.to_list(data[data['name'] == cryptoName]['first_historical_data'])[0]
          list_data[3] = dictS['firstListing']
          dictS['circulatingSupply'] = Market[-1].text
          list_data[0] = dictS['circulatingSupply']
          Supply = soup.find_all('div', class_ = 'maxSupplyValue')
          dictS['maxSupply'] = Supply[0].text
          list_data[1] = dictS['maxSupply']
          dictS['totalSupply'] = Supply[1].text
          list_data[2] = dictS['totalSupply']
          ''' github data'''
          sourceCode =soup.find_all('a', class_= "link-button")
          urlGit = ''
          for elt in sourceCode:
               if 'github' in elt['href']:
                    urlGit = elt['href']
                    break
          if urlGit != '':
               responseGit = dbconnect.requests.get(urlGit)
               soupGit = dbconnect.BeautifulSoup(responseGit.content, 'lxml')
               commits = soupGit.find_all('a', class_ = "pl-3 pr-3 py-3 p-md-0 mt-n3 mb-n3 mr-n3 m-md-0 Link--primary no-underline no-wrap")
               if commits != []:
                    dictS['Commits'] = (commits[0].text).split()[0]
                    list_data[4] = dictS['Commits']
               others = soupGit.find_all('div', class_ ='BorderGrid-cell')
               if others != []:
                    others = others[0].find_all('div',class_ ="mt-2")
                    dictS['Forks'] = (others[-1].text).split()[0]
                    list_data[5] = dictS['Forks']
                    dictS['Watching']= (others[-2].text).split()[0]
                    list_data[6] = dictS['Watching']
                    dictS['Stars']= (others[-3].text).split()[0]
                    list_data[7]  = dictS['Stars'] 
          dbconnect.setStat(cryptoName, dictS)
     except IndexError:
          dictS = {}
          dictS['firstListing']= list_data[0]
          dictS['circulatingSupply']= list_data[1]
          dictS['maxSupply']= list_data[2]
          dictS['totalSupply']= list_data[3]
          dictS['Commits']= list_data[4]
          dictS['Forks']= list_data[5]
          dictS['Watching']= list_data[6]
          dictS['Stars'] = list_data[7]
          dbconnect.setStat(cryptoName, dictS)

def getYearStatistics(cryptoName):
     '''
          collects the statistics from the current date to the first listing date for the given crypto
          @param: cryptoName is the name of the crypto that we want to scrap
          @param: CMC is the name of the collection where to insert data
     '''
     id = Series.to_list(data[data['name'] == cryptoName]['id'])[0]
     timeStart, timeEnd = dbconnect.datetime.now(), dbconnect.datetime.strptime(Series.to_list(data[data['name'] == cryptoName]['first_historical_data'])[0],'%Y-%m-%dT%H:%M:%S.000Z')
     delta = timeStart - timeEnd
     numberOfIterations = int(str(delta)[:str(delta).index('d')]) // 99
     for i in range(numberOfIterations):
          end = timeStart - dbconnect.timedelta(days = 99)
          end = str(dbconnect.datetime.timestamp(end))[:10]
          timeStart = str(dbconnect.datetime.timestamp(timeStart))[:10]
          urlCMC = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/historical?id="+str(id)+"&convertId=2781&timeStart="+end+"&timeEnd="+timeStart
          jsonData = dbconnect.requests.get(urlCMC).json()
          try : 
               allData = jsonData['data']
               quotes = allData['quotes']
               symbol = allData['symbol']
               for index in range(len(quotes)):
                    dbconnect.setHist(cryptoName,quotes[len(quotes)-1-index], CMC, 'CMC', False,symbol)
          except KeyError:
               pass
          timeStart = dbconnect.datetime.fromtimestamp(int(end))
     dbconnect.setYrCMC(cryptoName)
     
def scrap(cryptoName):
     ''' 
          this functions calls the two last functions to collect the data for the cryptoName
          either calls  year statistics or the  day statistics
          @param: cryptoName is the name of the crypto that we want to scrap
          @param: CMC is the name of the collection where to insert data
     '''
     if not dbconnect.getyrCMC(cryptoName) : 
          getYearStatistics(cryptoName)
          getDayStatistics(cryptoName)
     else:
          id = Series.to_list(data[data['name'] == cryptoName]['id'])[0]
          todayDate, yesterdayDate = dbconnect.datetime.now(), dbconnect.datetime.now() - dbconnect.timedelta(days = 2)
          end, start = str(dbconnect.datetime.timestamp(todayDate))[:10], str(dbconnect.datetime.timestamp(yesterdayDate))[:10]
          urlCMC = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/historical?id="+str(id)+"&convertId=2781&timeStart="+start+"&timeEnd="+end
          jsonData = dbconnect.requests.get(urlCMC).json()
          try :
               dayData = jsonData['data']
               quote = dayData['quotes']
               dbconnect.setHist(cryptoName, quote, CMC, 'CMC',True,'',True)
          except KeyError:
               pass
          ''' then we collect the data for the scraping day, notice that  at this time we don't have the good data structure'''
          getDayStatistics(cryptoName)

if __name__ == '__main__':
     logging.basicConfig(filename= r'src\stage\backend\database\scrapAndDbCMC.log', encoding='utf-8', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
     logging.info('---> Read and stored the dataframe of all coinmarketcap cyrptos in structure named data!!')
     logging.info('---> Established a connection to the database for the CoinMarketCap collection.')
     logging.info('---> Started the CoinMarketCap scraping.....')
     start = time.time()
     number_of_processes = 25
     number_of_cryptos = 100
     pool = Pool(number_of_processes)
     pool.map(scrap, dbconnect.list_names_cryptos[:100])
     logging.info('---> Finished the CoinMarketCap scraping, and the script took:'+str(time.time()-start))
