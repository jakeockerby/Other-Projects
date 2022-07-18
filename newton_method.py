# -*- coding: utf-8 -*-
"""
Created on Thu May  5 18:29:05 2022

@author: JakeOckerby
"""

import pandas as pd
import numpy as np
import scipy.stats as stat

S = 50
K = 47
r = 0.01
t = 0.5
sigma = 0.25
option = 'put'
error = 0.01
OP = 2.00
BSP = 0
iter_ = 2

count = 1
while abs(BSP - OP) > error:
# while count <= iter_:
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
        
    vega = S*(1/(2*np.pi)**(1/2))*np.exp(-(d1**2)/2)*t**(1/2)
    sigma = sigma + (OP - BSP)/vega
    
    count += 1
    print('d1 = ', d1)
    print('d2 = ', d2)
    print('phi(d1) = ', phi_d1)
    print('phi(d2) = ', phi_d2)
    print('phi(-d1) = ', phi_minusd1)
    print('phi(-d2) = ', phi_minusd2)
    print('\n')
    print('\n')
    print('Black-Scholes Price = £{}'.format(round(BSP, 2)))
    print('\n')
    print('Vega = ', vega)
    print('sigma = ', sigma)
    print('\n')
    print('\n')
    print('\n')