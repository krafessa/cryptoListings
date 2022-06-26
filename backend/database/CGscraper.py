'''
    scraps the COINGECKO website, and stores the data extracted in 
    the appropriate collection for the Cryptos Database
    @author : KRAFESS AYYOUB
    @date : 26-06-2022
'''

from bs4 import BeautifulSoup 
import sys
from selenium.common.exceptions import TimeoutException, NoSuchElementException, InvalidSessionIdException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.common.action_chains import ActionChains
import selenium.webdriver as webdriver
from multiprocessing import Pool
import logging
import dbconnect 
import json
from selenium.webdriver.chrome.options import Options
from functools import partial

CG = dbconnect.connect()['CoinGecko']
CMC = dbconnect.connect()['CoinMarketCap']

def browser():
    ''' 
        define the options to launch the driver for the chrome browser
        @return : a selenium drievr
    '''
    options = Options()
    userAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:101.0) Gecko/20100101 Firefox/101.0'
    options.add_argument(f'user-agent={userAgent}')
    driver = webdriver.Chrome(chrome_options=options, executable_path=r'C:\Users\AyyoubKRAFESS\Desktop\go-workspace\src\stage\backend\database\driver\chromedriver')
    return driver

def getDayStatistics(cryptoName):
    ''' 
        collects the daily statistics for a given crypto
        @param: cryptoName is the name of the crypto that we want to scrap
        @param: CG is the name of the collection where to insert data
    '''
    try:
        data = dbconnect.pd.read_pickle(r'src\stage\backend\database\Names&Symbols.pkl')
        slug = dbconnect.pd.Series.to_list(data[data['name'] == cryptoName]['slug'])[0]
        dictH = {}
        dictS = {}
        listTags = []
        urlCG = 'https://www.coingecko.com/en/coins/'+slug
        driverCG = browser()
        driverCG.get(urlCG)
        dictH['currentPrice'] = WebDriverWait(driverCG,10).until(EC.presence_of_all_elements_located((By.XPATH, '//span[@class="no-wrap"]')))[0].text
        dictH['lowestPrice'] = driverCG.find_element(by = By.XPATH,value = '//div[@class = "tw-text-gray-900 dark:tw-text-white tw-font-medium tw-col-span-1"]').text
        dictH['highestPrice'] = driverCG.find_element(by = By.XPATH,value = '//div[@class ="tw-text-gray-900 dark:tw-text-white tw-font-medium tw-col-span-1 tw-text-right"]').text
        dictH['volume'] = driverCG.find_elements(by = By.XPATH,value = '//span[@class = "tw-text-gray-900 dark:tw-text-white tw-font-medium"]')[0].text
        dictH['marketCap'] = driverCG.find_elements(by = By.XPATH,value='//span[@class = "tw-text-gray-900 dark:tw-text-white tw-font-medium"]')[1].text 
        tag = driverCG.find_element(by = By.XPATH,value= "//div[@class = 'dropdown center tw-h-7 tw-bg-gray-100 dark:tw-text-white dark:tw-bg-white dark:tw-bg-opacity-10 cursor-pointer tw-rounded-md']//a[@class = 'tw-px-2.5 tw-flex tw-items-center tw-justify-center tw-h-7 tw-text-sm tw-font-medium tw-text-gray-800 dark:tw-text-white dark:tw-bg-opacity-10 hover:tw-bg-gray-200 dark:hover:tw-bg-opacity-20 tw-rounded-l-md']").get_attribute('textContent')
        tags = driverCG.find_elements(by = By.XPATH,value = "//div[@class = 'dropdown-menu tw-py-1 dropdown-menu-right tw-max-h-52 overflow-auto tw-text-black lg:tw-min-w-56 tw-rounded-md tw-shadow-lg tw-border tw-border-black tw-border-opacity-5 dark:tw-border dark:tw-border-white dark:tw-border-opacity-12']//a[@class = 'dropdown-item tw-text-sm tw-pl-4 tw-py-2']")
        listTags.append(tag)
        if len(tags) != 0:
            for t in tags:
                listTags.append(t.get_attribute('textContent'))
        dictS['tags'] = listTags                
        driverCG.quit()
        dictH['date'] = dbconnect.datetime.now()
        dbconnect.setHist(cryptoName, dictH, CG, 'CG', '')
        dbconnect.setStat(cryptoName, dictS)
    except InvalidSessionIdException:
        driverCG.quit()
        dictH = {}
        dictH['currentPrice'] = 'BLANCK'
        dictH['lowestPrice'] = 'BLANCK'
        dictH['highestPrice'] = 'BLANCK'
        dictH['volume'] = 'BLANCK'
        dictH['marketCap'] = 'BLANCK'
        dictH['date'] = dbconnect.datetime.now()
    except NoSuchElementException:
        driverCG.quit()
        pass
    except TimeoutException:
        driverCG.quit()
        pass

def getYearStatistics(cryptoName):
    '''
        collects the statistics corresponding to one year for the given crypto
        @param: cryptoName is the name of the crypto that we want to scrap
        @param: CG is the name of the collection where to insert data
    '''
    data = dbconnect.pd.read_pickle(r'src\stage\backend\database\Names&Symbols.pkl')
    slug = dbconnect.pd.Series.to_list(data[data['name'] == cryptoName]['slug'])[0]
    try:
        startDate, endDate = (dbconnect.pd.Series.to_list(data[data['name'] == cryptoName]['first_historical_data'])[0])[:10], dbconnect.datetime.strftime(dbconnect.datetime.now(), '%Y-%m-%d')
        urlCG = 'https://www.coingecko.com/fr/pi%C3%A8ces/'+slug+'/historical_data?start_date='+startDate+'&end_date='+endDate+'#panel'
        driverCG = browser()
        driverCG.get(urlCG)
        numberOfPages = WebDriverWait(driverCG, 10).until(EC.presence_of_all_elements_located((By.XPATH,'//li[@class ="page-item"]')))[-1].text
        for j in range(1,int(numberOfPages)+1):
            driverCG = browser()
            urlCG = 'https://www.coingecko.com/fr/pi%C3%A8ces/'+slug+'/historical_data?start_date='+startDate+'&end_date='+endDate+'&page='+str(j)
            driverCG.get(urlCG)
            dataJson = dbconnect.pd.read_html(driverCG.page_source)[0]
            dictData = dataJson.to_dict()
            length = len(dictData['Date'])
            for k in range(length):
                if (k==0 and j==0):
                    pass
                else:
                    dictH = {}
                    dictH['date'] = dictData['Date'][k]
                    dictH['marketCap'] = dictData['Capitalisation boursière'][k]
                    dictH['volume'] = dictData['Volume'][k]
                    dictH['open'] = dictData["Cours d'ouverture"][k]
                    dictH['close'] = dictData["Cours de fermeture"][k]
                    dbconnect.setHist(cryptoName, dictH, CG, 'CG')
            driverCG.quit()
        dbconnect.setYrCG(cryptoName)
    except InvalidSessionIdException:
        pass

def scrap(cryptoName):
    ''' 
        this functions calls the two last functions to collect the data for the cryptoName
        either calls 1 year statistics or the 1 day statistics
        @param: cryptoName is the name of the crypto that we want to scrap
        @param: CG is the name of the collection where to insert data
     '''
    if not dbconnect.getyrCG(cryptoName) : 
        getYearStatistics(cryptoName)
    else :
        data = dbconnect.pd.read_pickle(r'src\stage\backend\database\Names&Symbols.pkl')
        slug = dbconnect.pd.Series.to_list(data[data['name'] == cryptoName]['slug'])[0]
        startDate, endDate = dbconnect.datetime.strftime(dbconnect.datetime.now() - dbconnect.timedelta(days = 1), '%Y-%m-%d' ), dbconnect.datetime.strftime(dbconnect.datetime.now(), '%Y-%m-%d')
        urlCG = 'https://www.coingecko.com/fr/pi%C3%A8ces/'+slug+'/historical_data?start_date='+startDate+'&end_date='+endDate+'#panel'
        driverCG = browser()
        driverCG.get(urlCG)
        dataJson = dbconnect.pd.read_html(driverCG.page_source)[0]
        driverCG.quit()
        dictData = dataJson.to_dict()
        dictH = {}
        dictH['date'] = dictData['Date'][1]
        dictH['marketCap'] = dictData['Capitalisation boursière'][1]
        dictH['volume'] = dictData['Volume'][1]
        dictH['open'] = dictData["Cours d'ouverture"][1]
        dictH['close'] = dictData["Cours de fermeture"][1]
        dbconnect.setHist(cryptoName, dictH, CG, 'CG', True)
        getDayStatistics(cryptoName)

if __name__ == '__main__':
    logging.basicConfig(filename= r'src\stage\backend\database\scrapAndDbCMC.log', encoding='utf-8', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    data = dbconnect.pd.read_pickle(r'src\stage\backend\database\Names&Symbols.pkl')
    logging.info('---> Read and stored the dataframe of all coinmarketcap cyrptos in structure named data!!')
    CG = dbconnect.connect()['CoinGecko']
    CMC = dbconnect.connect()['CoinMarketCap']
    logging.info('---> Established a connection to the database for the CoinMarketCap collection.')
    logging.info('---> Started the CoinGecko scraping.....')
    start = dbconnect.time.time()
    number_of_processes = 5
    number_of_cryptos = 100
    number_of_iterations = number_of_cryptos//number_of_processes
    pool = Pool(number_of_processes)
    for iter in range(number_of_iterations):
        pool.map(getDayStatistics, dbconnect.list_names_cryptos[iter*number_of_processes:(iter+1)*number_of_processes])
    pool.terminate()
    logging.info('---> Finished the CoinGecko scraping, and the script took:'+str(dbconnect.time.time()-start))
