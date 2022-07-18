# -*- coding: utf-8 -*-
"""
Created on Fri May  6 14:24:58 2022

@author: JakeOckerby
"""

import pandas as pd
import numpy as np
import scipy.stats as stat

S = 183
K = 200
r = 0.02
t = 1/4
sigma_low = 0.2
sigma_high = 0.4
option = 'put'
error = 0.01
OP = 5
BSP = 0
iter_ = 2

count = 1
# while abs(BSP - OP) > error:
while count <= iter_:
    sigma = (sigma_low + sigma_high)/2
    d1 = (np.log(S/K) + (r + (sigma**2)/2)*t)/(sigma*t**(1/2))
    d2 = d1 - sigma*t**(1/2)
    
    phi_d1 = stat.norm.cdf(d1)
    phi_minusd1 = 1 - stat.norm.cdf(d1)
    phi_d2 = stat.norm.cdf(d2)
    phi_minusd2 = 1 - stat.norm.cdf(d2)
    
    if option == 'put':
        BSP = phi_minusd2*K*np.exp(-r*t) - phi_minusd1*S
    else:
        BSP = phi_d1*S - phi_d2*K*np.exp(-r*t)
        
    if OP - BSP < 0:
        sigma_high = sigma
    else:
        sigma_low = sigma
    
    count += 1
    print('sigma mid = ', sigma)
    print('d1 = ', d1)
    print('d2 = ', d2)
    print('phi(d1) = ', phi_d1)
    print('phi(d2) = ', phi_d2)
    print('phi(-d1) = ', phi_minusd1)
    print('phi(-d2) = ', phi_minusd2)
    print('\n')
    print('\n')
    print('Black-Scholes Price = {}p'.format(round(BSP, 2)))
    print('\n')
    print('sigma low = ', sigma_low)
    print('sigma high = ', sigma_high)
    print('\n')
    print('\n')
    print('\n')