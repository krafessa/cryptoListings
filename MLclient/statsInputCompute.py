'''
    This module offers various functions to compute different statistics properties such as median, variance, sharpe ratio ...
    @author : KRAFESS AYYOUB
    @date : 18-08-2022
'''
import statistics as st

def minValue(list_values):
    ''' computes the min value of a list of prices...'''
    return min(list_values)

def maxValue(list_values):
    ''' computes the max value of a list of prices...'''
    return max(list_values)

def meanValue(list_values):
    ''' computes the mean value of a list o prices...'''
    return st.mean(list_values)

def varianceValue(list_values):
    ''' computes the variance value of a list of prices...'''
    return st.variance(list_values)

def sharpeRatioValue(list_values, rf):
    ''' computes the Sharpe ratio for the given list of prices'''
    returns = []
    if len(list_values)>2:
        returns=[(list_values[i+1]-list_values[i])/list_values[i] for i in range(len(list_values)-1)]
    risk_free_rate = rf
    final_return = st.mean(returns)
    volatility = st.stdev(returns)
    return (final_return-risk_free_rate)/volatility

    