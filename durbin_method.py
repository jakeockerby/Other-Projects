# -*- coding: utf-8 -*-
"""
Created on Sun May  8 09:20:25 2022

@author: JakeOckerby
"""

import pandas as pd
import numpy as np
import scipy.stats as stat

lags = 3
rho = [0.8, 0.5, 0.4]

phi = np.zeros((lags, lags))
phi[0][0] = rho[0]

for k in range(lags-1):
    rhophi = [phi[k][j]*rho[k-j] for j in range(k+1)]
    rhophisum = sum(rhophi)
    phisum = [phi[k][j]*rho[j] for j in range(k+1)]
    phisum = sum(phisum)
    
    phi[k+1][k+1] = (rho[k+1] - rhophisum)/(1 - phisum)
    for j in range(k+1):
        phi[k+1][j] = phi[k][j] - phi[k+1][k+1]*phi[k][k-j]