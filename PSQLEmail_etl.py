#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 29 12:56:53 2020

@author: Alaisha Naidu
Name: ETL Functions for Email Table - PostgreSQL Database
"""

import pandas as pd 
from pandas import DataFrame
import sqlalchemy
import psycopg2


#Extract

connection_uri = 'postgresql://user:password@localhost:5432/client_information'
db_engine = sqlalchemy.create_engine(connection_uri)

#extact table to a Pandas Dataframe
def extract_tables_to_pandas(tablename, db_engine):
    query = "SELECT * FROM {}".format(tablename)
    emails_df = pd.read_sql(query, db_engine)
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

#load transformed data into a PostgreSQL database
def load_df_to_dwh(email_df, tablename, schema, db_engine):
    loaded_data = pd.to_sql(tablename, db_engine, schema = schema, if_exists = "append")
    return loaded_data
    

db_engines = {...} #needs to be configured
def email_etl(db_engine, tablename):
    #Extract
    email_df = extract_tables_to_pandas("email", db_engines["client_information"])
    #Transfrom
    split_emails = split_emails_transformation(email_df)
    frequency_count = email_analysis(email_df)
    #Load
    load1 = load_df_to_dwh(split_emails, "Split Emails", db_engines["dwh"])
    load2 = load_df_to_dwh(frequency_count, "Frequency Count", db_engines["dwh"])
    return load1, load2


