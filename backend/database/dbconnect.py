''' connect to the Database
    + all the auxiliary functions used for accessing and updating the database
     @author : KRAFESS AYYOUB
     @date : 28-05-2022
'''
from multiprocessing import connection
import pandas as pd
import pymongo 
from bs4 import BeautifulSoup
from requests import request
import logging
import time
import requests
from selenium import webdriver 
from datetime import datetime, timedelta

def connect():
    ''' 
        establish a connection to mongodb and returns a session to access the Cryptos 
        database.
    '''
    try:
        myclient = pymongo.MongoClient('localhost', 27017)
        return myclient['Cryptos']
    except pymongo.errors.ConnectionFailure as e:
         print("---> Could not connect to  Mongodb!!!", e)

def setHist(name,dict, mycol,source,unique,symbol = '', delete = False):
    ''' 
        Adds the new dictionary containing the historical data for a given crypto.
        @param: name is the name of the crypto for which we will update the hist data
        @param: symbol is the symbol of the crypto
        @param: dict is a dictionary containing the new data
        @param: mycol is the collection in where do our modification
        @param: delete to know if we modified a the last date to respect the usual data structure
                (by default takes False)
    '''
    mycolHist1,mycolHist2,mycolHist3 = mycol['Historical Prices A->H'], mycol['Historical Prices I->P'], mycol['Historical Prices Q->Z']
    name = name.upper()
    Hist = getHist(name, mycol)
    if not delete:
        if unique == False:
            Hist.append(dict)
        else:
            Hist.insert(0,dict)
    else:
        if source == 'CMC':
            Hist[0] = dict[0]
    if ord(name[0]) >= ord('A') and ord(name[0]) <= ord('H'):
        if symbol != '':
            mycolHist1.update_one({'name' : name, 'symbol':symbol},{"$set" : {'Hist' : Hist}}, upsert = True)
        else:
            mycolHist1.update_one({'name' : name},{"$set" : {'Hist' : Hist}}, upsert = True)
    elif ord(name[0]) >= ord('I') and ord(name[0]) <= ord('P'):
        if symbol != '':
            mycolHist2.update_one({'name' : name, 'symbol':symbol},{"$set" : {'Hist' : Hist}}, upsert = True)
        else:
            mycolHist2.update_one({'name' : name},{"$set" : {'Hist' : Hist}}, upsert = True)
    else:
        if symbol != '':
            mycolHist3.update_one({'name' : name, 'symbol':symbol},{"$set" : {'Hist' : Hist}}, upsert = True)
        else:
            mycolHist3.update_one({'name' : name},{"$set" : {'Hist' : Hist}}, upsert = True)

def getHist(name, mycol):
    '''
        Return the list of historical data related to the name given as a parameter
        if name doesn't exist, returns an empty list after inserting the crypto to the 
        appropriate database
        @param: name is the name of the crypto for which we will update the hist data
        @param: mycol is the collection in where do our modification
        @return: list
    '''
    mycolHist1,mycolHist2,mycolHist3 = mycol['Historical Prices A->H'], mycol['Historical Prices I->P'], mycol['Historical Prices Q->Z']
    listHist = list()
    if ord(name[0]) <= ord('H') and ord(name[0]) >= ord('A'):
        query = mycolHist1.find_one({'name' : name})
        if query != None:
            listHist = query['Hist']
        return listHist
    elif ord(name[0]) <= ord('P') and ord(name[0]) >= ord('I'):
        query = mycolHist2.find_one({'name' : name})
        if query != None:
            listHist = query['Hist']
        return listHist
    else:
        query = mycolHist3.find_one({'name' : name})
        if query != None:
            listHist = query['Hist']
        return listHist        

def setStat(name, dict):
    '''
        Adds the new dictionary containing the statical data for a given crypto.
        If a portion of the data is the same no need to modify the crypto
        @param: name is the name of the crypto for which we will update the hist data
        @param: dict is a dictionary containing the new data
    '''
    mydb = connect()
    name = name.upper()
    mycolStat1,mycolStat2,mycolStat3 = mydb['Static Data A->H'], mydb['Static Data I->P'], mydb['Static Data Q->Z']
    if ord(name[0]) >= ord('A') and ord(name[0]) <= ord('H'): 
        mycolStat1.update_one({'name' : name}, {'$set' : dict}, upsert = True)
    elif ord(name[0]) >= ord('I') and ord(name[0]) <= ord('P'):
        mycolStat2.update_one({'name' : name}, {'$set' : dict}, upsert = True)
    else:
        mycolStat3.update_one({'name' : name}, {'$set' : dict}, upsert = True)

def getStat(name):
    '''
        returns the static data for a given crypto
        @param: name is the name of the crypto for which we will update the hist data       
    '''
    mycol = connect()
    mycolStat1,mycolStat2,mycolStat3 = mycol['Static Data A->H'], mycol['Static Data I->P'], mycol['Static Data Q->Z']
    if ord(name[0]) <= ord('H') and ord(name[0]) >= ord('A'):
        query = mycolStat1.find_one({'name' : name})
        return query
    elif ord(name[0]) <= ord('P') and ord(name[0]) >= ord('I'):
        query = mycolStat2.find_one({'name' : name})
        return query 
    else:
        query = mycolStat3.find_one({'name' : name})
        return query           

def setYrCMC(name):
    ''' 
        if we collected the histdata, sets the attribute year statistics CMC to true
        @param: name is the name of the crypto for which we will update the hist data
    '''
    mycol = connect()
    name = name.upper()
    mycolStat1,mycolStat2,mycolStat3 = mycol['Static Data A->H'], mycol['Static Data I->P'], mycol['Static Data Q->Z']
    if ord(name[0]) >= ord('A') and ord(name[0]) <= ord('H'): 
        mycolStat1.update_one({'name' : name}, {'$set' : {'year statistics CMC' : True}}, upsert = True)
    elif ord(name[0]) >= ord('I') and ord(name[0]) <= ord('P'):
        mycolStat2.update_one({'name' : name}, {'$set' : {'year statistics CMC' : True}}, upsert = True)
    else:
        mycolStat3.update_one({'name' : name}, {'$set' : {'year statistics CMC' : True}}, upsert = True)

def getyrCMC(name):
    ''' 
        returns a boolean to know either the crypto has a list of one year statistics for the
        CoinMarketCap collection
        @param: name is the name of the crypto for which we will update the hist data
        @return: boolean
    '''
    name =name.upper()
    query = getStat(name)
    if query != None:
        try:
            val = query['year statistics CMC']
            return val
        except KeyError as e :
            return False
    else:
        return False

def setYrCG(name):
    ''' 
        if we collected the histdata, sets the attribute year statistics CG to true
        @param: name is the name of the crypto for which we will update the hist data
    '''
    mycol = connect()
    name = name.upper()
    mycolStat1,mycolStat2,mycolStat3 = mycol['Static Data A->H'], mycol['Static Data I->P'], mycol['Static Data Q->Z']
    if ord(name[0]) >= ord('A') and ord(name[0]) <= ord('H'): 
        mycolStat1.update_one({'name' : name}, {'$set' : {'year statistics CG' : True}}, upsert = True)
    elif ord(name[0]) >= ord('I') and ord(name[0]) <= ord('P'):
        mycolStat2.update_one({'name' : name}, {'$set' : {'year statistics CG' : True}}, upsert = True)
    else:
        mycolStat3.update_one({'name' : name}, {'$set' : {'year statistics CG' : True}}, upsert = True)

def getyrCG(name):
    ''' 
        returns a boolean to know either the crypto has a list of one year statistics for the 
        CoinGecko collection
        @return: boolean
    '''
    name = name.upper()
    query = getStat(name)
    if query != None:
        try:
            val = query['year statistics CG']
            return val
        except KeyError as e :
            return False
    else:
        return False

list_names_cryptos = ['Bitcoin', 'Ethereum', 'Tether', 'USD Coin', 'BNB', 'Cardano', 'XRP', 'Binance USD', 'Solana', 'Polkadot', 'Dogecoin', 'Avalanche', 'Dai', 'Polygon', 'Shiba Inu', 'Uniswap', 'TRON', 'Wrapped Bitcoin', 'Ethereum Classic', 'UNUS SED LEO', 'Litecoin', 'FTX Token', 'Chainlink', 'NEAR Protocol', 'Cronos', 'Cosmos', 'Stellar', 'Flow', 'Monero', 'Bitcoin Cash', 'Algorand', 'Filecoin', 'VeChain', 'ApeCoin', 'Internet Computer', 'Decentraland', 'The Sandbox', 'Tezos', 'Theta Network', 'Hedera', 'Axie Infinity', 'Quant', 'Elrond', 'Aave', 'EOS', 'TrueUSD', 'Bitcoin SV', 'Helium', 'Maker', 'OKB', 'Zcash', 'KuCoin Token', 'RChain', 'Fantom', 'IOTA', 'The Graph', 'Pax Dollar', 'BitTorrent', 'Chiliz', 'eCash', 'Klaytn', 'Neo', 'Lido DAO', 'Curve DAO Token', 'USDD', 'Neutrino USD', 'Swap', 'Waves', 'Stacks', 'Huobi Token', 'Basic Attention Token', 'Loopring', 'Enjin Coin', 'Zilliqa', 'PAX Gold', 'Dash', 'STEP', 'Mina', 'Kusama', 'Decred', 'Kava', 'Oasis Network', 'Celo', 'Bitcoin Gold', 'Arweave', 'Trust Wallet Token', 'Synthetix', '1inch Network', 'Convex Finance', 'NEM', 'Holo', 'Optimism', 'Compound', 'Qtum', 'Gala', 'yearn.finance', 'Nexo', 'Gnosis', 'Fei USD', 'Ravencoin', 'Bitcoin', 'Ethereum', 'Tether', 'USD Coin', 'BNB', 'Cardano', 'XRP', 'Binance USD', 'Solana', 'Polkadot', 'Dogecoin', 'Avalanche', 'Dai', 'Polygon', 'Shiba Inu', 'Uniswap', 'TRON', 'Wrapped Bitcoin', 'Ethereum Classic', 'UNUS SED LEO', 'Litecoin', 'FTX Token', 'Chainlink', 'NEAR Protocol', 'Cronos', 'Cosmos', 'Stellar', 'Flow', 'Monero', 'Bitcoin Cash', 'Algorand', 'Filecoin', 'VeChain', 'ApeCoin', 'Internet Computer', 'Decentraland', 'The Sandbox', 'Tezos', 'Theta Network', 'Hedera', 'Axie Infinity', 'Quant', 'Elrond', 'Aave', 'EOS', 'TrueUSD', 'Bitcoin SV', 'Helium', 'Maker', 'OKB', 'Zcash', 'KuCoin Token', 'RChain', 'Fantom', 'IOTA', 'The Graph', 'Pax Dollar', 'BitTorrent', 'Chiliz', 'eCash', 'Klaytn', 'Neo', 'Lido DAO', 'Curve DAO Token', 'USDD', 'Neutrino USD', 'Swap', 'Waves', 'Stacks', 'Huobi Token', 'Basic Attention Token', 'Loopring', 'Enjin Coin', 'Zilliqa', 'PAX Gold', 'Dash', 'STEP', 
'Mina', 'Kusama', 'Decred', 'Kava', 'Oasis Network', 'Celo', 'Bitcoin Gold', 'Arweave', 'Trust Wallet Token', 'Synthetix', '1inch Network', 'Convex Finance', 'NEM', 'Holo', 'Optimism', 'Compound', 'Qtum', 'Gala', 'yearn.finance', 'Nexo', 'Gnosis', 'Fei USD', 'Ravencoin', 'Kadena', 'XDC Network', 'GateToken', 'Celsius', 'IoTeX', 'Amp', 'Theta Fuel', 'Ethereum Name Service', 'BORA', 'OMG Network', 'Ankr', 'Harmony', 'TerraClassicUSD', 'ICON', 'Audius', '0x', 'Symbol', 'JUST', 'Livepeer', 'WOO Network', 'OST', 'Kyber Network Crystal v2', 'Immutable X', 'Serum', 'Golem', 'Balancer', 'Storj', 'Moonbeam', 'Ontology', 'Hive', 'Bitcoin Standard Hashrate Token', 'Horizen', 'SKALE Network', 'Siacoin', 'WAX', 'SXP', 'Smooth Love Potion', 'Polymath', 'UMA', 'Gemini Dollar', 'Braintrust', 'Secret', 'Chia', 'SwissBorg', 'Swap', 'DigiByte', 'Render Token', 'CEEK VR', 'Casper', 'Dogelon Mars', 'NFT', 'PlayDapp', 'MXC', 'ConstitutionDAO', 'Nervos Network', 'Civic', 'Keep Network', 'Celer Network', 'Pundi X (New)', 'Flux', 'dYdX', 'Ren', 'Lisk', 'Acala Token', 'MediBloc', 'Nano', 'NuCypher', 'Constellation', 'WINkLink', 'Reserve Rights', 'Ontology Gas', 'Rally', 'Request', 'Ellipsis', 'Orbs', 'Ocean Protocol', 'Numeraire', 'Bancor', 'Conflux', 'Powerledger', 'Chromia', 'MX TOKEN', 'COTI', 'Function X', 'Syscoin', 'API3', 'Dent', 'JOE', 'Frax Share', 'Status', 'Spell Token', 'Biconomy', 'XYO', 'Ardor', 'Raydium', 'Coin98', 'DAO Maker', 'Cartesi', 'Vulcan Forged PYR', 'HEX', 'Wrapped TRON', 'Lido Staked ETH', 'yOUcash', 'Bitcoin BEP2', 'X', 'Bit', 'Toncoin', 'Frax', 'Wrapped BNB', 'BitTorrent', 'Huobi BTC', 'Terra Classic', 'DeFiChain', 'The Transfer Token', 'Tether Gold', 'GensoKishi Metaverse', 'NXM', 'Threshold', 'WEMIX', 'Osmosis', 'Counos X', 'Rocket Pool', 'Fruits', 'LINK', 'Safe', 'Terra', 'BinaryX', 'LooksRare', 'Astar', 'Metis', 'VVS Finance', 'ZEON', 'Liquity USD', 'Baby Doge Coin', 'HUSD', 'Humanscape', 'Uquid Coin', 'MaidSafeCoin', 'RadioCaca', 'Everscale', 'Project Galaxy', 'Chainbing', 'STASIS EURO', 'Meta', 'MVL', 'PlatonCoin', 'Aurora', 'LUKSO', 'Anyswap', 'sUSD', 'Injective', 'USDX [Kava]', 'Steem', 'inSure DeFi', 'Stratis', 'Adshares', 'Pirate Chain', 'AVINOC', 'Venus USDC', 'Biswap', 'Voyager Token', 'NEST Protocol', 'Revain', 'Seedify.fund', 'Perpetual Protocol', 'MobileCoin', 'Boba Network', 'Telcoin', 'Augur', 'VeThor Token', 'Ultra', 'Shentu', 'Velas', 'aelf', 'Metal', 
'Yield Guild Games', 'OVR', 'WazirX', 'Bifrost', 'ssv.network', 'Wrapped Velas', 'renBTC', 'Centrifuge', 'Venus', '1eco', 'StormX', 'FUNToken', 'Mdex', 'Origin Protocol', 'Persistence', 'Reef', 'Orchid', 'Ampleforth Governance Token', 'Cred', 'MyNeighborAlice', 'OriginTrail', 'iExec RLC', 'Orbit Chain', 'Hxro', 'Alien Worlds', 'Aragon', 'DEAPcoin', 'Radicle', 'NKN', 'Alpha Venture DAO', 'Alchemy Pay', 'Veritaseum', 'Freeway Token', 'Fetch.ai', 'Dawn Protocol', 'Rakon', 'Utrust', 'Liquity', 'Mines of Dalarnia', 'Quark', 'Sologenic', 'Illuvium', 'RSK Smart Bitcoin', 'Energy Web Token', 'UFO Gaming', 'Rari Governance Token', 'SUP', 'Tribe', 'RSK Infrastructure Framework', 'BakeryToken', 'Maple', 'Ark', 'SOMESING', 'Beta Finance', 'Decentralized Social', 'DFI.Money', 'Strike', 'Metadium', 'Sun (New)', 'Flamingo', 'Wirex Token', 'MovieBloc', 'Everipedia', 'Locus Chain', 'Ergo', 'Propy', 'Dusk Network', 'Verasity', 'GlitzKoin', 'Polkastarter', 'Swap', 'Loom Network', 'Band Protocol', 'Bridge Oracle', 'Splintershards', 'DeFi Pulse Index', 'CENNZnet', 'XSGD', 'Aavegotchi', 'MOBOX', 'Aleph.im', 'BarnBridge', 'Verge', 'TomoChain', 'Badger DAO', 'HedgeTrade', 'Cult DAO', 'Enzyme', 'Hoo Token', 'cVault.finance', 'Electroneum', 'Samoyedcoin', 'Venus BUSD', 'Ampleforth', 'Tellor', 'Aergo', 'Divi', 'Ribbon Finance', 'ThunderCore', 'Neutrino Token', 'ARPA Chain', 'ASD', 'World Mobile Token', 'dKargo', 'YooShi', 'Marlin', 'Vai', 'RAMP', 'Chrono.tech', 'Mask Network', 'Cocos-BCX', 'IDEX', 'Sport and Leisure', 'Dero', 'HUNT', 'LCX', 'Stargate Finance', 'Mrweb Finance', 'Telos', 'Orion Protocol', 'SingularityNET', 'Hyperion', 'Efforce', 'JasmyCoin', 'AXEL', 'XCAD Network', 'XMON', 'e-Radix', 'ABBC Coin', 'Starlink', 'Celo Dollar', 'Origin Dollar', 'Mango', 'Proton', 'Wanchain', 'Ankr Reward Bearing Staked ETH', 'Rise', 'KOK', 'ONUS', 'ZB Token', 'Safe', 'TrueFi', 'Hifi Finance', 'H2O DAO', 'AIOZ Network', 'WhiteCoin', 
'Akash Network', 'LCX', 'Metahero', 'Vega Protocol', 'Elastos', 'RichQUACK.com', 'Karura', 'Gitcoin', 'Carry', 'Decentral Games', 'Linear Finance', 'Bella Protocol', 'Bloktopia', 'FLETA', 'Wing Finance', 'Travala.com', 'Komodo', 'Chimpion', 'Syntropy', 'REI Network', 'ONBUFF', 'Anchor Protocol', 'Unifi Protocol DAO', 'Alpha Quark Token', 'XeniosCoin', 'mStable USD', 'Alpaca Finance', 'Moss Coin', 'Wilder World', 'TROY', 'Assemble Protocol', 'LTO Network', 'DIA', 'Super Zero Protocol', 'CONUN', 'BitShares', 'Litentry', 'Merit Circle', 'Qcash', 'BurgerCities', 'Automata Network', 'KardiaChain', 'Harvest Finance', 'CLV', 'Ethernity', 'PlatON', 'BitMart Token', 'GXChain', 'Phantasma', 'Steem Dollars', 'STAKE', 'Toko Token', 'Cobak Token', 'Sentinel Protocol', 'RSS3', 'Dvision Network', 'apM Coin', 
'MonaCoin', 'Phala Network', 'Kava Lend', 'Bluzelle', 'IRISnet', 'Beefy Finance', 'Pitbull', 'FirmaChain', 'Hathor', 'Refereum', 'RMRK', 'Venus XVS', 'Unibright', 'Bitcoin Diamond', 'MiL.k', 'Alpine F1 Team Fan Token', 'Tranchess', 'Celo Euro', 'OpenDAO', 'RIZON', 'CONTRACOIN', 'Klever', 'Contentos', 'Adventure Gold', 'FIO Protocol', 'Presearch', 'district0x', 'Firo', 'GMT Token', 'Boson Protocol', 'Tornado Cash', 'Keep3rV1', 'Wrapped NXM', 'Venus USDT', 'CoinLoan', 'Aeternity', 'Router Protocol', 'Tokenlon Network Token', 'Rarible', 'Qredo', 'Nestree', 'Shapeshift FOX Token', 'Cortex', 'Alitas', 'SuperRare', 'DxChain Token', 'Streamr', 'Gas', 'Swap', 'bZx Protocol', 'Mithril', 'Efinity Token', 'Handshake', 'HyperDAO', 'Groestlcoin', 'Deeper Network', 'Swap', 'TokenPocket', 'Reserve', 'Alethea Artificial Liquid Intelligence Token', 'USDK', 'Genopets', 'Swarm', 'AMO Coin', 'Voxies', 'Ambire AdEx', 'CUDOS', 'S.S. Lazio Fan Token', 'TerraKRW', 'Swarm', 'MANTRA DAO', 'LATOKEN', 'BTU Protocol', 'Frontier', 'Paris Saint-Germain Fan Token', 'ICHI', 'FC Porto Fan Token', 'Somnium Space Cubes', 'Hydra', 'smARTOFGIVING', 'HI', 'VerusCoin', 'Velo', 'Student Coin', 'NULS', 'KILT Protocol', 'PARSIQ', 'SelfKey', 'StaFi', 'ZIMBOCASH', 'ReapChain', 'Akropolis', 'Maro', 'Gifto', 'Morpheus.Network', 'Swap', 'Highstreet', 'Bonfida', 'Elitium', 'Energi', 'MixMarvel', 'MEVerse', 'Beam', 'Waltonchain', 'Circuits of Value', 'Drep [new]', 'Ultiledger', 'Grid+', 'AhaToken', 'DerivaDAO', 'Time New Bank', 'Bytom', 'CoinEx Token', 'MAP Protocol', 'FC Barcelona Fan Token', 'Measurable Data Token', 'DeRace', 'DeXe', 'Marinade Staked SOL', 'Kin', 'DXdao', 'MimbleWimbleCoin', 'SHPING', 'DAD', 'Vectorspace AI', 'TiFi Token', 'Valobit', 'SOLVE', 'Bounce Finance Governance Token', 'ION', 'Kleros', 'Santos FC Fan Token', 'Manchester City Fan Token', 'Ooki Protocol', 'Covalent', 'Dego Finance', 'Shiba Predator', 'Lattice Token', 'Dock', 'Green Satoshi Token (SOL)', 'Haven Protocol', 'pNetwork', 'League of Kingdoms Arena', 'DODO', 'Fusion', 'GYEN', 'ApolloX', 'BTSE', 'Cratos', 'dForce', 'MATH', 'Hoge Finance', 'GET Protocol', 'PEAKDEFI', 'SIX', 'Defi', 'Misbloc', 'HOPR', 'TABOO TOKEN', 'Arcblock', 'Aurory', 'Auto', 'Mirror Protocol', 'CargoX', 'PowerPool', 'Suku', 'Kryll', 'VIDT Datalink', 'BOSAGORA', 'Hermez Network', 'Swap', 'RAI Finance', 'BASIC', 'DigitalBits', 'Victoria VR', 'Star Atlas', 'Gari Network', 
'Peony', 'DEXTools', 'VITE', 'MileVerse', 'Wrapped NCG (Nine Chronicles Gold)', 'Krypton DAO', 'Altura', 'WaykiChain', 'TE-FOOD', 'NFT', 'Nimiq', 'Apollo Currency', 'QASH', 'Electric Vehicle Zone', 'Woodcoin', 'Solend', 'ForTube', 'Metronome', 'Xeno Token', 'AnimalGo', 'AirSwap', 'Adappter Token', 'BoringDAO', 'DigixDAO', 'SingularityDAO', 'BitForex Token', 'Revolution Populi', 'RFOX', 'ERC20', 'Cellframe', 'WHALE', 'BIDR', 'YIELD App', 'TokenClub', 'Namecoin', 'USDJ', 'Quantstamp', 'BSCPAD', 'Civilization', 'Rupiah Token', 'Kava Swap', 'ReddCoin', 'AllianceBlock', 'Pluton', 'Zenon', 'Cryptex Finance', 'New BitShares', 'Cream Finance', 
'Jupiter', 'Oxen', 'Polkadex', 'BioPassport Token', 'MILC Platform', 'BitKan', 'LBRY Credits', 'Rai Reflex Index', 'Lossless', 'Obyte', 'Crypterium', 'SifChain', 'SENSO', 'Rubic', 'Handy', 'Soda Coin', 'Aurox', 'Rainicorn', 'PERL.eco', 'AC Milan Fan Token', 'Saito', 'FOAM', 'Sovryn', 'Star Atlas DAO', 'GameFi', 'Dora Factory', 'ZKSpace', 'PIVX', 'CoinPoker', 'Epic Cash', 'Quiztok', 'Neblio', 'Sylo', 'NerveNetwork', 'Ternoa', 'GoChain', 'X World Games', 'ScPrime', 'Quantum Resistant Ledger', 'Shiden Network', 'TEMCO', 'Ariva', 'ELYSIA', 'Swap', 'Thetan Arena', 'Gamium', 'Position Exchange', 'Shyft Network', 'Inverse Finance', 'Edge', 'VIMworld', 'Bloomzed Loyalty Club Ticket', 'TrustVerse', 'Bytecoin', 'GamerCoin', 'Counterparty', 'NewYork Exchange', 'Zebec Protocol', 'Atletico De Madrid Fan Token', 'Inter Milan Fan Token', 'Peercoin', 'HI', 'Monavale', 'Santiment Network Token', 'Vivid Labs', 'Unisocks', 'Venus Reward Token', 'Solanium', 'GuildFi', 'Carbon', 'pSTAKE Finance', 'Gods Unchained', 'Observer', 'Cere Network', 'Pendle', 'King DAG', 'Newscrypto', 'Vertcoin', 'BEPRO Network', 'Bitrue Coin', 'Hiblocks', 'Sentinel', 'ChainX', 'UniCrypt', 'Aventus', 'Validity', '0Chain', 'Permission Coin', 'Banano', 'Zynecoin', 'Arianee', 'e-Money', 'XDEFI Wallet', 'Galatasaray Fan Token', 'Hacken Token', 'Era Token (Era7)', 'Invictus Hyperion Fund', 'Birake', 'DeFine', 'SwftCoin', 'Visor.Finance', 'AS Roma Fan Token', 'AntiMatter Governance Token', 'Huobi Pool Token', 'mStable Governance Token: Meta (MTA)', 'Carbon Credit', 'Impossible Finance Launchpad', 'MAPS', 'GMCoin', 'Cajutel', 'Cyclub', 'Project WITH', 'Molecular Future', 'Hamster', 'saffron.finance', 'Everest', 'Diamond', 'Goldfinch', 'PolkaFoundry', 'Nash', 'Woonkly Power', 'Bitcoin 2', 'Lunar', 'Numbers Protocol', 'Dragonchain', 'ProximaX', 'TouchCon', 'Darma Cash', 'Callisto Network', 'BullPerks', 'NFT Worlds', 'Pallapay', 'BUX Token', 'ChainGuardians', 'OG Fan Token', 'Monero Classic', 'Cashaa', 'Mysterium', 'Glitch', 'O3 Swap', 'Grin', 'Bone ShibaSwap', 'ShareToken', 'Juventus Fan Token', 'Strike', 'UniLend', 'Agoras: Currency of Tau', 'Atari Token', 'PearDAO', 'ASTA', 'TNC Coin', 'Ekta', 'X', 'TriumphX', 'Dovu', 'Pangolin', 'HyperCash', 'All Sports', 'Seele-N', 'Forta', 'Swap', 'Town Star', '#MetaHash', 'Blockchain Brawlers', 'Ambrosus', 'AAX Token', 'Emirex Token', 'Navcoin', 'DIGG', 'Wabi', 'POA Network', 'Valor Token', 'Bitball Treasure', 'InsurAce', 'Stratos', 'StackOs', 'Unicly CryptoPunks Collection', 'DeFi Yield Protocol', 'HAPI Protocol', 'BLOCKv', 'Populous', 'Nakamoto Games', 'ProBit Token', 'Poseidon Network', 'QuadrantProtocol', 'Blocery', 'Receive Access Ecosystem', 'Sperax']