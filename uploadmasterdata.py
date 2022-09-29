import os
import pandas as pd
from IPython.display import display
import datetime
import pyodata
import requests
import json
import time

def write_dict_transportl_from_csv(row,dic):
  dic["NavBG7SOURCELOCATION"].append({
      "LOCFR" :str(row["LOCFR"]),
      "PRDID": row["PRDID"],
      "LEADTIMETRANSPORTATION": str(row["LEADTIMETRANSPORTATION"]),
      "TRATIO": str(row["TRATIO"]),
      "LOCID": str(row["LOCID"])
    })


def upload_transportl(cl):
  location_prod = pd.read_csv("pds_transport_lanes_timeseries.csv",sep=";")
  dic= {}
  dic["TransactionID"]=str(datetime.datetime.now())+"transl"
  dic["RequestedAttributes"]="LOCFR,PRDID,LEADTIMETRANSPORTATION,TRATIO,LOCID"
  dic["DoCommit"]=True
  dic["NavBG7SOURCELOCATION"]=[]
  location_prod.apply(lambda x: write_dict_transportl_from_csv(x,dic),axis=1)
  create_request = cl.entity_sets.BG7SOURCELOCATIONTrans.create_entity()
  create_request.set(**dic)
  create_request.execute() 


def write_dict_depdemand_from_csv(row,dic):
  dic["NavBG7"].append({
      "PRDID": str(row["PRDID"]),
      "LOCID": str(row["LOCID"]),
      "INDEPENDENTDEMAND" : str(row ["INDEPENDENTDEMAND"]),
      "PERIODID0_TSTAMP" : str(row ["KEYFIGUREDATE"])+"T7:07:30"
    })


def write_dict_prod_from_csv(row,dic):
  dic["NavBG7PRODUCT"].append({
      "PRDID": row["PRDID"],
      "PRDDESCR": row["PRDDESCR"]
    })


def write_dict_pds_comp_from_csv(row,dic):
  dic["NavBG7PRODUCTIONSOURCEITM"].append({
      "PRDID": row["PRDID"],
      "SOURCEID": row["SOURCEID"],
      "COMPONENTCOEFFICIENT" : str(row ["COMPONENTCOEFFICIENT"])
    })


def write_dict_pds_header_from_csv(row,dic):
  dic["NavBG7SOURCEPRODUCTION"].append({
      "PRDID": row["PRDID"],
      "LOCID": str(row["LOCID"]),
      "SOURCEID": row["SOURCEID"],
      "PLEADTIME" : str(row["PLEADTIME"]),
      "PMAXLOTSIZE" : str(row["PMAXLOTSIZE"]),
      "PMINLOTSIZE" : str(row["PMINLOTSIZE"]),
      "PRATIO" : str(row["PRATIO"]),
      "OUTPUTCOEFFICIENT" : str(row ["OUTPUTCOEFFICIENT"]),
      "SOURCETYPE" : row["SOURCETYPE"]
    })

def write_dict_location_from_csv(row,dic):
  dic["NavBG7LOCATION"].append({"LOCID":str(row["LOCID"]),
      "LOCDESCR": row["LOCDESCR"],
      "LOCTYPE":row["LOCTYPE"]})

def write_dict_locationprod_from_csv(row,dic):
  dic["NavBG7LOCATIONPRODUCT"].append({
      "PRDID": row["PRDID"],
      "LOCID": str(row["LOCID"])
    })

def upload_products(cl):
  products= pd.read_csv("PRODUCT_timeseries.csv",sep=";")
  dic= {}
  dic["TransactionID"]=str(datetime.datetime.now())+"product"
  dic["RequestedAttributes"]="PRDID,PRDDESCR"
  dic["DoCommit"]=True
  dic["NavBG7PRODUCT"]=[]
  products.apply(lambda x: write_dict_prod_from_csv(x,dic),axis=1)
  create_request = cl.entity_sets.BG7PRODUCTTrans.create_entity()
  create_request.set(**dic)
  create_request.execute() 


def upload_locations(cl):
  location= pd.read_csv("locations_timeseries.csv",sep=";")
  dic= {}
  dic["TransactionID"]=str(datetime.datetime.now())+"location"
  dic["RequestedAttributes"]="LOCID,LOCDESCR,LOCTYPE"
  dic["DoCommit"]=True
  dic["NavBG7LOCATION"]=[]
  location.apply(lambda x: write_dict_location_from_csv(x,dic),axis=1)
  create_request = cl.entity_sets.BG7LOCATIONTrans.create_entity()
  create_request.set(**dic)
  create_request.execute() 

def upload_locationprod(cl):
  location_prod = pd.read_csv("product_location_timeseries.csv",sep=";",error_bad_lines=False)
  dic= {}
  dic["TransactionID"]=str(datetime.datetime.now())+"product_location"
  dic["RequestedAttributes"]="PRDID,LOCID"
  dic["DoCommit"]=True
  dic["NavBG7LOCATIONPRODUCT"]=[]
  location_prod.apply(lambda x: write_dict_locationprod_from_csv(x,dic),axis=1)
  create_request = cl.entity_sets.BG7LOCATIONPRODUCTTrans.create_entity()
  create_request.set(**dic)
  create_request.execute() 

def upload_pds_header(cl):
  pds_header=pd.read_csv("pds_header_additionals_timeseries.csv",sep=";",error_bad_lines=False)
  dic= {}
  dic["TransactionID"]=str(datetime.datetime.now())+"product_location"
  dic["RequestedAttributes"]="PRDID,SOURCEID,LOCID,PLEADTIME,PMAXLOTSIZE,PMINLOTSIZE,PRATIO,OUTPUTCOEFFICIENT,SOURCETYPE"
  dic["DoCommit"]=True
  dic["NavBG7SOURCEPRODUCTION"]=[]
  pds_header.apply(lambda x: write_dict_pds_header_from_csv(x,dic),axis=1)
  create_request = cl.entity_sets.BG7SOURCEPRODUCTIONTrans.create_entity()
  create_request.set(**dic)
  create_request.execute() 

def upload_pds_comp(cl):
  pds_comp=pd.read_csv("pds_comp_timeseries.csv",sep=";")
  dic= {}
  dic["TransactionID"]=str(datetime.datetime.now())+"product_location"
  dic["RequestedAttributes"]="PRDID,SOURCEID,COMPONENTCOEFFICIENT"
  dic["DoCommit"]=True
  dic["NavBG7PRODUCTIONSOURCEITM"]=[]
  pds_comp.apply(lambda x: write_dict_pds_comp_from_csv(x,dic),axis=1)
  create_request = cl.entity_sets.BG7PRODUCTIONSOURCEITMTrans.create_entity()
  create_request.set(**dic)
  create_request.execute() 



def upload_indep_demand():
  with open('auth.txt') as f:
    auth = [tuple(map(str, i.split(','))) for i in f]
  inddemand=pd.read_csv("pds_demand_key_figures_timeseries.csv",sep=";")
  url = 'https://o22-001-api.devsys.net.sap/sap/opu/odata/IBP/PLANNING_DATA_API_SRV/BG7Trans'
  myobj = {
  "Transactionid": str(datetime.datetime.now()),
  'AggregationLevelFieldsString': 'LOCID,PRDID,INDEPENDENTDEMAND,PERIODID0_TSTAMP',
  "DoCommit": True,
  #'VersionID': 'B',
  'NavBG7': []
  }
  inddemand.apply(lambda x: write_dict_depdemand_from_csv(x,myobj),axis=1)
  s = requests.Session()
  r = s.head(url,auth=auth[0], headers={'x-csrf-token': 'fetch'},verify=False)
  token = r.headers.get('x-csrf-token','')
  #print(token)
  s.headers.update({'x-csrf-token': token})
  x = s.post(url, json = myobj, auth=auth[0], verify=False)


def write_dict_init_keyfig_from_csv(row,dic):
  dic["NavBG7"].append({
      "PRDID": str(row["PRDID"]),
      "LOCID": str(row["LOCID"]),
      "INITIALINVENTORY" : str(row ["INITIALINVENTORY"]),
      "PERIODID0_TSTAMP" : str(row ["KEYFIGUREDATE"])+"T7:07:30"
    })


def upload_init_keyfigures():
  with open('auth.txt') as f:
    auth = [tuple(map(str, i.split(','))) for i in f]
  inddemand=pd.read_csv("pds_init_key_figures_timeseries.csv",sep=";")
  url = 'https://o22-001-api.devsys.net.sap/sap/opu/odata/IBP/PLANNING_DATA_API_SRV/BG7Trans'
  myobj = {
  "Transactionid": str(datetime.datetime.now())+"init",
  'AggregationLevelFieldsString': 'LOCID,PRDID,INITIALINVENTORY,PERIODID0_TSTAMP',
  "DoCommit": True,
  #'VersionID': 'B',
  'NavBG7': []
  }
  inddemand.apply(lambda x: write_dict_init_keyfig_from_csv(x,myobj),axis=1)
  s = requests.Session()
  r = s.head(url,auth=auth[0], headers={'x-csrf-token': 'fetch'},verify=False)
  token = r.headers.get('x-csrf-token','')
  #print(token)
  s.headers.update({'x-csrf-token': token})
  x = s.post(url, json = myobj, auth=auth[0], verify=False)


def upload_all_data():
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

  upload_products(upload_master_data_cl)

  upload_locations(upload_master_data_cl)
  time.sleep(10)
  upload_locationprod(upload_master_data_cl)
  upload_transportl(upload_master_data_cl)
  upload_pds_header(upload_master_data_cl)
  upload_pds_comp(upload_master_data_cl)
  #time.sleep(25)
  upload_init_keyfigures()
  upload_indep_demand()


#upload_all_data()