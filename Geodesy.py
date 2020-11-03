#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  3 19:15:22 2020

@author: Alaisha Naidu
Name: Geodesy Gravity Data ETL
Creds: UCT

"""
#First Order
import pandas as pd 
from pandas import DataFrame
import numpy as np

#Extract
table = pd.read_excel(r'/Users/user/Desktop/DataPoints.xlsx')

list = []

#Transform
for i in range(len(table)):
    new_list =[]
    new_list.append(1)
    new_list.append(table.Latitude__deg_[i])
    new_list.append(table.Longitude__deg_[i])
    list.append(new_list)

A = np.matrix(list)
P = np.identity(500)

fc_list = []
ac_list = []

for i in range(len(table)):
    fc_l = []
    ac_l = []
    fc_l.append(table.Free_Air_Correction[i])
    ac_l.append(table.Atmospheric_Correction[i])
    fc_list.append(fc_l)
    ac_list.append(ac_l)
    