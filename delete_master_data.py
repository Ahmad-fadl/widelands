import os
import pandas as pd
from IPython.display import display
import datetime
import pyodata
import requests
import json
import time

def write_dict_location_from_csv(row,dic):
  dic["NavBG7LOCATION"].append({"LOCID":str(row["LOCID"]),
      "LOCDESCR": row["LOCDESCR"],
      "LOCTYPE":row["LOCTYPE"]})

def upload_locations(cl):
  location= pd.read_csv("locations_timeseries.csv",sep=";")
  dic= {}
  dic["TransactionID"]=str(datetime.datetime.now())+"location"
  dic["RequestedAttributes"]="LOCID,LOCDESCR,LOCTYPE"
  dic["DoCommit"]=True
  dic["DeleteEntries"]=True
  dic["NavBG7LOCATION"]=[]
  location.apply(lambda x: write_dict_location_from_csv(x,dic),axis=1)
  create_request = cl.entity_sets.BG7LOCATIONTrans.create_entity()
  create_request.set(**dic)
  create_request.execute() 

def write_dict_prod_from_csv(row,dic):
  dic["NavBG7PRODUCT"].append({
      "PRDID": row["PRDID"],
      "PRDDESCR": row["PRDDESCR"]
    })

def upload_products(cl):
  products= pd.read_csv("PRODUCT_timeseries.csv",sep=";")
  dic= {}
  dic["TransactionID"]=str(datetime.datetime.now())+"product"
  dic["RequestedAttributes"]="PRDID,PRDDESCR"
  dic["DeleteEntries"]=True
  dic["DoCommit"]=True
  dic["NavBG7PRODUCT"]=[]
  products.apply(lambda x: write_dict_prod_from_csv(x,dic),axis=1)
  create_request = cl.entity_sets.BG7PRODUCTTrans.create_entity()
  create_request.set(**dic)
  create_request.execute() 

def delete_master_data():
  SERVICE_URL = 'https://o22-001-api.devsys.net.sap/sap/opu/odata/IBP/MASTER_DATA_API_SRV'


  session = requests.Session()
  session.verify = False
  with open('auth.txt') as f:
      auth = [tuple(map(str, i.split(','))) for i in f]
  session.auth = auth[0]
  response = session.head(SERVICE_URL, headers={'x-csrf-token': 'fetch'})
  token = response.headers.get('x-csrf-token', '')
  session.headers.update({'x-csrf-token': token})
  upload_master_data_cl = pyodata.Client(SERVICE_URL, session)
  upload_locations(upload_master_data_cl)
  upload_products(upload_master_data_cl)

delete_master_data()