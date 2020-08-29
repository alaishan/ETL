#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 29 12:56:53 2020

@author: Alaisha Naidu
Name: ETL Functions for Email Table - CSV
"""

import pandas as pd 
from pandas import DataFrame
import csv

#Extract

pathname = '/Users/user/Desktop/SampleData.csv'

#data from a CSV on my desktop (located via pathname)
def extract_tables_to_df(pathname1):
    emails_df = pd.read_csv(pathname1, sep = ";", usecols=["Email"])
    return emails_df


#Transform

#Split email into username and domain 
def split_emails_transformation(emails_df):
    split_email = emails_df.Email.str.split("@", expand = True)
    emails_df = emails_df.assign(
        username=split_email[0],
        domain=split_email[1],
        )
    return emails_df

#Frequency count of domains
def email_analysis(emails_df):
    frequency = emails_df['domain'].value_counts()
    return frequency


#Load

#data to a CSV (saved to desktop via pathname)
def load_to_csv(emails_df, frequency):
   return emails_df.to_csv('/Users/user/Desktop/emails.csv'), frequency.to_csv('/Users/user/Desktop/emailsfrequency.csv')


def email_etl(pathname):
    #Extract
    email_df = extract_tables_to_df(pathname)
    #Transfrom
    split_emails = split_emails_transformation(email_df)
    frequency_count = email_analysis(email_df)
    #Load
    csvs = load_to_csv(split_emails, frequency_count)
    return csvs


