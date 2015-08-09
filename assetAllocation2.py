# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Jul 21 20:38:41 2015

@author: RDUKTRADING
Refer to page 330 of Python for finance
""" 

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
#
#symbols = ['AAPL','MSFT','YHOO','DB','GLD']

data = pd.read_csv('~/Desktop/data.csv')
data = data.dropna()
data['Date'] =  pd.to_datetime(data['Date'])
data = data.set_index('Date')
data = data.resample('M',how='last',fill_method='ffill')
data = data.drop('Cash_US',1)
#data = data.drop('Bar_US_Tre_TR',1)
#data = data.drop('Bar_US_HY_TR',1)
#data = data.drop('SPX_TR',1)

#for sym in symbols:
#        data[sym]=web.DataReader(sym,data_source='yahoo',end='2014-09-12')['Adj Close']
#data.columns=symbols

"""Gets the daily returns and plots them"""
(data/data.ix[0]*100).plot(figsize=(8,5))
noa= len(data.columns)

"""Calculates the daily log returns"""
rets=np.log(data/data.shift(1))
rets = rets.ix[1:]
"""Annualises the returns"""
rets.mean()*12 #not sure this does anything since variable is not stored

"""Calculates the annual covariance matrix"""
a = rets.cov()*12 #not sure this does anything since variable is not stored

"""Generates random weights that sum to 1"""
weights=np.random.random(noa)
weights/=np.sum(weights)

"""Calculates the portfolio expected return"""
np.sum(rets.mean()*weights)*12

"""Calculates the portfolio variance"""
np.dot(weights.T,np.dot(rets.cov()*12,weights))#not sure this does anything since variable is not stored

"""Calculates the Standard Deviation"""
np.sqrt(np.dot(weights.T,np.dot(rets.cov()*12,weights)))#not sure this does anything since variable is not stored

"""Function will give back the major portfolio stats (will be used later)"""

def statistics(weights):
    """Returns portfolio statistics
    
    Parameters
    ==========
    weights : array-like
        weights for different securities in portfolio
        
    Returns
    =======
    pret : float
        expected portfolio return
    pvol : float
        expected portfolio volatility
    pret/pvol : float
        Sharpe Ratio for rf=0
    """
    weights=np.array(weights)
    pret=np.sum(rets.mean()*weights)*12
    pvol=np.sqrt(np.dot(weights.T,np.dot(rets.cov()*12,weights)))
    return np.array([pret,pvol,pret/pvol])

"""Portfolio optimisation to minimise variance"""
import scipy.optimize as sco

def min_func_variance(weights): # dont think this is the min varaince
        return statistics(weights)[1]**2

"""Constraints on the portfolio i.e. sum of weights =1"""
cons=({'type':'eq','fun':lambda x: np.sum(x)-1})

#"""Bounds on the portfolio i.e. each weight must be non negative"""
bnds=tuple((0,1) for x in range(noa))
#
optv=sco.minimize(min_func_variance,noa*[1./noa],method='SLSQP',bounds=bnds,constraints=cons)

"""Prints the output"""
#optv
#        

"""Section works on the efficient frontier"""
#
#"""new constraints and bounds (trets is target return level btw)"""
#cons=({'type':'eq','fun':lambda x: statistics(x)[0]-tret}),{'type':'eq','fun':lambda x: np.sum(x)-1}
bnds=tuple((0,1) for x in weights)

""" Function to minimise vol"""
def min_func_port(weights):
    print weights
    return statistics(weights)[2]

trets=np.linspace(0.07,0.10,2)
tvols=[]
for tret in trets:
    cons=({'type':'eq','fun':lambda x: statistics(x)[0]-tret}),{'type':'eq','fun':lambda x: np.sum(x)-1}
    res = sco.minimize(min_func_port,noa*[1./noa,],method='SLSQP',bounds=bnds,constraints=cons)
    tvols.append(res['fun'])

tvols=np.array(tvols)

plt.figure(figsize=(8,4))
plt.scatter(tvols,trets,c=trets/tvols,marker='x')
    #efficient frontier
plt.plot(statistics(optv['x'])[1],statistics(optv['x'])[0],'y*',markersize=15.0)
    #minimum variance portfolio
plt.grid(True)
plt.xlabel('expected vol')
plt.ylabel('expected return')
plt.colorbar(label='Sharpe ratio')