#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 30 11:46:22 2020

@author: Alaisha Naidu
Name: DAG (Directly Acyclic Graph)
Creds: DataCamp
"""
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator 

dag = DAG(dag_id = "emails",
          schedule_interval = "0 0 * * *") #Schedule interval in cron notation to execute daily

task_emails = PythonOperator(
    task_id = "emails_task",
    python_callable = email_etl,
    )
