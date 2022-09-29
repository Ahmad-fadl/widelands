import pandas as pd
import pyodata
import requests
import json
def download_dep_Demand():
  SERVICE_URL = 'https://o22-001-api.devsys.net.sap/sap/opu/odata/IBP/EXTRACT_ODATA_SRV/'


  session = requests.Session()
  session.verify = False
  with open('auth.txt') as f:
      auth = [tuple(map(str, i.split(','))) for i in f]
  session.auth = auth[0]
  response = session.head(SERVICE_URL, headers={'x-csrf-token': 'fetch'})
  token = response.headers.get('x-csrf-token', '')
  session.headers.update({'x-csrf-token': token})
  cl = pyodata.Client(SERVICE_URL, session)

  data=cl.entity_sets.BG7.get_entities().select('PRDID,LOCID,DEPENDENTDEMAND,LOCDESCR').execute()

  keyfigures= pd.DataFrame(columns=['PRDID','LOCID','LOCDESCR','DEPENDENTDEMAND'])
  counter=0
  for i in data:
    if ((not i.LOCDESCR.endswith("warehouse"))&(not i.LOCDESCR.endswith("headquarters"))):
      #print(i.LOCDESCR)
      #print(i.LOCID)
      keyfigures.loc[counter] =[i.PRDID,i.LOCID,i.LOCDESCR,i.DEPENDENTDEMAND]
      counter+=1

  keyfigures.to_csv("new_depdemand.csv",index=False,sep=";")

#download_dep_Demand()