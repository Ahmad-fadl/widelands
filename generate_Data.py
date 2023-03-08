#read all files into an array (dataframes)
import numpy as np
import os
import pandas as pd
from IPython.display import display
import datetime
import re
import json


def filter_one_or_zero(n):
  return 1 if (n) else 0 

def split_wares(wares):
  if wares=="{}":
    return None
  res = re.sub(r".*wares\:", "", str(wares))
  return json.loads(res)

def split_inputqueus(inputqueu):
  if (len(inputqueu)==2):
    return dict({"none":0})

  try:
    res = json.loads(inputqueu)
    for i in res.keys():
      res[i] = json.loads(res[i])[2]
    return res
  except:
    return 'error:' + str(inputqueu) 
#  res = json.loads(inputqueu)
#  for i in res.keys():
#    res[i] = json.loads(res[i])[2]
#  return res
      

def read_data_from_files(directory):
  dataframes= []
  for root,dirs,files in os.walk(directory):
      for file in files:
         if (file.endswith("Buildingslogs.csv") & os.path.exists(file)):
            col_names = pd.read_csv(file, nrows=0).columns

            dataframes.append(pd.read_csv(file,sep=";",low_memory=False,header=[0],skiprows=[1],error_bad_lines=False))
  return dataframes


#group the data by building where d[str(serial)] = group

def group_data_by_building(data):
  d = {}
  for data in data :
    for serial, group in data.groupby('serial'):
      d[str(serial)] = group
  return d


def int_to_time(input,hours_mapping):
  return datetime.datetime(2024, 1, 1) + datetime.timedelta(hours=input/hours_mapping)

def hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +datetime.timedelta(hours=t.minute//30))  

def map_to_real_time(data,index):
  # hou much is an hour in game time 
  # for example 1hour = 2000 game time unite
  res=data
  hour_in_numbers = (int(data[index]["time"].max()) - int(data[index]["time"].min())) / (365*24)
  for df in res :
    df.drop(index=df.index[-1],axis=0,inplace=True)
    df["time"]=df["time"].apply(int_to_time,args=[hour_in_numbers])
    #df["time"]=df["time"].apply(hour_rounder)
    df.time=df.time.round("0.1S")
  return res  

def read_orders_data_from_files(directory):
  dataframes= []
  for root,dirs,files in os.walk(directory):
      for file in files:
         if (file.endswith("orderslogs.csv") & os.path.exists(file)):
            col_names = pd.read_csv(file, nrows=0).columns

            dataframes.append(pd.read_csv(file,sep=";",low_memory=False,header=[0],skiprows=[1],error_bad_lines=False))
  return dataframes




def write_series_of_location_to_csv_as_IBPlocation(locationseries_nameseries):

  IBPlocations = pd.DataFrame(columns=['LOCID','GEOLATITUDE','GEOLONGITUDE','HOLDINGCOSTPCT','LOCBUPAID','LOCDESCR','LOCIDDISPLAY','LOCREGION','LOCTYPE','LOCVALID','PROFILESETID'])
  IBPlocations[["LOCID","LOCDESCR"]]=locationseries_nameseries
  

  IBPlocations["LOCTYPE"]="P"

  #IBPlocations.drop_duplicates(subset ="LOCNO",keep = False, inplace = True)
  IBPlocations.to_csv("locations_timeseries.csv",sep=";",index=False,header=True)


def write_product_location(serial,wares,input_queues,is_warehouse,is_production_site):
  if is_warehouse:
    IBPproduct_location = pd.DataFrame(columns=["PRDID","LOCID","PLUNITID"])
    wares=wares
    wares = re.sub(r".*wares\:", "", wares)
    Dict = json.loads(wares)
    wares_amount_data = pd.DataFrame.from_dict(Dict.items())
    wares_amount_data.columns = ['wares', 'amount']
    IBPproduct_location["PRDID"]=wares_amount_data["wares"]
    #IBPproduct_location["MNG01"]=wares_amount_data["amount"]
    IBPproduct_location['LOCID']=str(serial)
    #IBPproduct_location["PLUNITID"]="SPA"
    #IBPproduct_location["VRFKZ"]="X"
    IBPproduct_location.to_csv("product_location_timeseries.csv",sep=";",index=False,mode='a',header=not os.path.exists("product_location_timeseries.csv"))
  if is_production_site:
    IBPproduct_location = pd.DataFrame(columns=["PRDID","LOCID","PLUNITID"])
    wares=input_queues
    Dict = json.loads(wares)
    wares_amount_data=pd.DataFrame(columns=['wares', 'amount'])
    wares_amount_data['wares'] = Dict.keys()
    
    IBPproduct_location["PRDID"]=wares_amount_data["wares"]
    #IBPproduct_location["MNG01"]=wares_amount_data["amount"]
    IBPproduct_location['LOCID']=str(serial)
    #IBPproduct_location["PLUNITID"]="SPA"
    #IBPproduct_location["VRFKZ"]="X"
    IBPproduct_location.to_csv("product_location_timeseries.csv",sep=";",index=False,mode='a',header=not os.path.exists("product_location_timeseries.csv"))

def write_singel_string_wares_attribute_to_csv_as_IBPproduct(wares):
  IBPmaterial = pd.DataFrame(columns=['PRDID','PRDDESCR','PRDFAMILY','PRDSUBFAMILY','PRDGROUP','MATTYPEID','UOMDESCR','UOMID'])

  wares = re.sub(r".*wares\:", "", wares)
  Dict = json.loads(wares)
  wares_amount_data = pd.DataFrame.from_dict(Dict.items())

  wares_amount_data.columns = ['wares', 'amount']
  IBPmaterial["PRDID"]=wares_amount_data["wares"]
  IBPmaterial["PRDDESCR"]=wares_amount_data["wares"]
  IBPmaterial["UOMID"]="ST"
  IBPmaterial=IBPmaterial.append({'PRDID':'stones', 'PRDDESCR':"stones", 'UOMID':'ST'}, ignore_index=True)
  
  

  return IBPmaterial

def write_one_buildings_entry_to_IBPstock(building_entry,filename):
  IBPstocks = pd.DataFrame(columns=['MANDT','DELKZ','LOGSYS','MATNR','LOC_WERKS','LGORT','PLAAB','BLANR','LOC_LIFNR','LOC_KUNNR','CHARG','TST01','VRFKZ','MNG01'])
  wares=building_entry["wares"]
  wares = re.sub(r".*wares\:", "", wares)
  Dict = json.loads(wares)
  wares_amount_data = pd.DataFrame.from_dict(Dict.items())
  wares_amount_data.columns = ['wares', 'amount']
  IBPstocks["MATNR"]=wares_amount_data["wares"]
  IBPstocks["MNG01"]=wares_amount_data["amount"]
  IBPstocks['LOC_WERKS']=str(building_entry["serial"])
  IBPstocks["VRFKZ"]="X"
  IBPstocks['MANDT']="001"
  IBPstocks['LOGSYS']="WDL"
  IBPstocks.to_csv(filename+"stocks.csv",sep=";",index=False,header=False)

def write_one_buildings_orders_to_IBPorders(orders):
  IBPorders = pd.DataFrame(columns=['MANDT','LOGSYS','DELNR','DELPS','DELET','DELKZ','MATNR','LOC_WERKS',
  'CHARG','PLAAB','PLANR','LGORT','LOC_LIFNR','LOC_KUNNR','TST01','VRFKZ','MNG01','MNG02',
  'MNG03','TST02','FIX01','LOC_WERKS_FROM','VERID','DELVR','AUFVR','POSVR',
  'INFNR','EKORG','ESOKZ','MNG04','DEPLOYMENT','MOT_ID','EINVR','DPS_TST','PP_STAGE','REL_FIXED_STATUS'])
  IBPorders['MANDT']="001"
  IBPorders['LOGSYS']="WDL"
  IBPorders['DELNR']=orders.serial.apply(str)+orders.name.apply(str)+orders.time.apply(str)+orders.requested_Ware.apply(str)+orders.amount.apply(str)
  IBPorders.to_csv("IBPorders.csv")

  
def rewrite_wares_to_one_simgle_ware(wares,ware):
  return wares[ware]

def json_and_index(string,index):
  return json.loads(string)[index]


def write_pds_header(serial,wares,input_queues,is_warehouse,is_production_site):
  if is_warehouse:
    data_to_be_written_to_csv = pd.DataFrame(columns=['SOURCEID','PRDID','LOCID','OUTPUTCOEFFICIENT','OUTPUTCOEFFICIENTTS','PDELIVERYTYPE','PINCLOTSIZE',
    'PLEADTIME','PMAXLOTSIZE','PMINLOTSIZE','PRATIO','PRATIOTS','SOURCETYPE','PCAPACONSPOLICY'])

    if (wares != "na"):
        wares = re.sub(r".*wares\:", "", wares)
        Dict = json.loads(wares)
        wares_amount_data = pd.DataFrame.from_dict(Dict.items())
        wares_amount_data.columns = ['wares', 'amount']
        data_to_be_written_to_csv["PRDID"]=wares_amount_data["wares"]
        data_to_be_written_to_csv["OUTPUTCOEFFICIENT"]=1
    else: return
    data_to_be_written_to_csv["SOURCEID"]= wares_amount_data["wares"].apply(lambda x : x+str(serial))
    
    #IBPproduct_location["MNG01"]=wares_amount_data["amount"]
    data_to_be_written_to_csv['LOCID']=str(serial)
    
    data_to_be_written_to_csv["PLEADTIME"]=1
    #data_to_be_written_to_csv["PDSPRIO"]=1
    data_to_be_written_to_csv["PMAXLOTSIZE"]=50
    data_to_be_written_to_csv["PMINLOTSIZE"]=1
    data_to_be_written_to_csv["PRATIO"]=1
    data_to_be_written_to_csv["SOURCETYPE"]="P"
  
    data_to_be_written_to_csv.to_csv("pds_header_time_series.csv",sep=";",index=False,mode='a',header=not os.path.exists("pds_header_time_series.csv"))


  if is_production_site:
    data_to_be_written_to_csv = pd.DataFrame(columns=['SOURCEID','PRDID','LOCID','OUTPUTCOEFFICIENT','OUTPUTCOEFFICIENTTS','PDELIVERYTYPE','PINCLOTSIZE',
    'PLEADTIME','PMAXLOTSIZE','PMINLOTSIZE','PRATIO','PRATIOTS','SOURCETYPE','PCAPACONSPOLICY'])
    if (len(input_queues)<3):
      return
    wares=input_queues
    Dict = json.loads(wares)
    wares_amount_data = pd.DataFrame.from_dict(Dict.items())
    wares_amount_data.columns = ['wares', 'amount']
    data_to_be_written_to_csv["PRDID"]=wares_amount_data["wares"]

    #print(type(wares_amount_data["amount"].iloc[0]))
    #print(wares_amount_data["amount"])
    data_to_be_written_to_csv["OUTPUTCOEFFICIENT"]=1
    
    data_to_be_written_to_csv["PRDID"]=wares_amount_data["wares"]
    #IBPproduct_location["MNG01"]=wares_amount_data["amount"]
    data_to_be_written_to_csv["SOURCEID"]= wares_amount_data["wares"].apply(lambda x : x+str(serial))
    
    #IBPproduct_location["MNG01"]=wares_amount_data["amount"]
    data_to_be_written_to_csv['LOCID']=str(serial)
    
    data_to_be_written_to_csv["PLEADTIME"]=1
    #data_to_be_written_to_csv["PDSPRIO"]=1
    data_to_be_written_to_csv["PMAXLOTSIZE"]=50
    data_to_be_written_to_csv["PMINLOTSIZE"]=1
    data_to_be_written_to_csv["PRATIO"]=1
    data_to_be_written_to_csv["SOURCETYPE"]="P"
    #data_to_be_written_to_csv["OUTPUTCOEFFICIENT"]=1
    data_to_be_written_to_csv.to_csv("pds_header_time_series.csv",sep=";",index=False,mode='a',header=not os.path.exists("pds_header_time_series.csv"))
  
 


def read_production_programms_data_from_files(directory):
  dataframes= []
  for root,dirs,files in os.walk(directory):
      for file in files:
         if (file.endswith("productionprograms.csv") & os.path.exists(file)):
            col_names = pd.read_csv(file, nrows=0).columns

            dataframes.append(pd.read_csv(file,sep=";",low_memory=False,header=None,error_bad_lines=False,
            names=["serial","name","programm_name","programm_descname","produced_Wares","recruited_workers","consumed_wares_workers","time"]))
  return dataframes


def create_activity_id(x):
  return x["input_wares"]+"+"+x["produced_wares"]+str(x["serial"])

def creat_component_id(x):
  return x["input_wares"]+":"+x["amount"] +"+"+x["produced_wares"]+"C"


def write_pds_comp(serial,producedwares,inputwares,programm_descname):
    data_to_be_written_to_csv = pd.DataFrame(columns=['SOURCEID','PRDID','SOURCEITMID','COMPONENTCOEFFICIENT','COMPONENTCOEFFICIENTTS','COMPONENTOFFSET'])

    if (producedwares != "{}"):
        wares = re.sub(r".*wares\:", "", producedwares)
        Dict = json.loads(wares)
    # wares_amount_data = pd.DataFrame(columns=['produced_wares','amount'])
        wares_amount_data = pd.DataFrame(list(Dict.items()),columns=['produced_wares','amount'])
        #print(wares_amount_data)
        wares_amount_data = wares_amount_data[['produced_wares']]
        #print(wares_amount_data)
    else: return

    if (inputwares != "{}"):
      wares = re.sub(r".*wares\:", "", inputwares)
      Dict = json.loads(wares)
      wares_amount_input_data = pd.DataFrame(list(Dict.items()),columns= ['input_wares', 'amount'])
    else: return

    merged_data=wares_amount_data.merge(wares_amount_input_data,how="cross")

    #print(merged_data)
    merged_data["id"]=range(len(merged_data))
    merged_data["serial"]=serial
    #print("fuck",merged_data)
    #IBPproduct_location["MNG01"]=wares_amount_data["amount"]
    data_to_be_written_to_csv["SOURCEID"]=merged_data["produced_wares"].apply(lambda x : x+str(serial))
    data_to_be_written_to_csv["PRDID"]=merged_data["input_wares"]
    data_to_be_written_to_csv["COMPONENTCOEFFICIENT"]=merged_data["amount"]


    data_to_be_written_to_csv.to_csv("pds_comp_timeseries.csv",sep=";",index=False,mode='a',header=not os.path.exists("pds_comp_timeseries.csv"))



def write_product_location_from_production_programs(inputrow):
  
  IBPproduct_location = pd.DataFrame(columns=["PRDID","LOCID","PLUNITID"])
  if (len(inputrow["produced_Wares"])<3):
    return

  IBPproduct_location["PRDID"]=list(json.loads(inputrow["produced_Wares"]).keys())
  #IBPproduct_location["MNG01"]=wares_amount_data["amount"]
  IBPproduct_location['LOCID']=inputrow["serial"]
  #IBPproduct_location["PLUNITID"]="ST"
  #IBPproduct_location["VRFKZ"]="X"
  IBPproduct_location.to_csv("product_location_timeseries.csv",sep=";",index=False,mode='a',header=not os.path.exists("product_location_timeseries.csv"))



def write_pds_header_additionals(inputrow):

    data_to_be_written_to_csv = pd.DataFrame(columns=['SOURCEID','PRDID','LOCID','OUTPUTCOEFFICIENT','OUTPUTCOEFFICIENTTS','PDELIVERYTYPE','PINCLOTSIZE',
    'PLEADTIME','PMAXLOTSIZE','PMINLOTSIZE','PRATIO','PRATIOTS','SOURCETYPE','PCAPACONSPOLICY'])


    data_to_be_written_to_csv["PRDID"]=list(json.loads(inputrow["produced_Wares"]).keys())
    data_to_be_written_to_csv["SOURCEID"]= data_to_be_written_to_csv["PRDID"].apply(lambda x : x+str(inputrow["serial"]))
    #IBPproduct_location["MNG01"]=wares_amount_data["amount"]
    data_to_be_written_to_csv['LOCID']=inputrow["serial"]
    data_to_be_written_to_csv["PLEADTIME"]=1
    #data_to_be_written_to_csv["PDSPRIO"]=1
    data_to_be_written_to_csv["PMAXLOTSIZE"]=50
    data_to_be_written_to_csv["PMINLOTSIZE"]=1
    data_to_be_written_to_csv["PRATIO"]=1
    data_to_be_written_to_csv["OUTPUTCOEFFICIENT"]=1
    #IBPproduct_location["VRFKZ"]="X"
    data_to_be_written_to_csv["SOURCETYPE"]="P"
    data_to_be_written_to_csv.to_csv("pds_header_additionals_timeseries.csv",sep=";",index=False,mode='a',header=not os.path.exists("pds_header_additionals_timeseries.csv"))

#ADDLANEID;MOTID;PRDID;LOCFR;LOCTO;GOODSRECEIPTPROCTIME;INBOUNDCALID;LEADTIME;TMAXLOTSIZE;TMINLOTSIZE


def write_pds_transport_lanes(inputrow):
    data_to_be_written_to_csv = pd.DataFrame(columns=['LOCFR','LOCID','PRDID','INBOUNDCALID','IOTFROZENWINDOW','LEADTIMETRANSPORTATION',
    'LEADTIMETRANSPORTATIONCV','MATTRSOURCELOCATION','RATIOTS','TBALANCERECEIPTSCOPE','TCAPACONSPOLICY','TDELIVERYTYPE','TFREEZEHORIZON','TINCLOTSIZE','TINVALID',
    'TLEADTIME','TLEADTIMEVARIABILITY','TLOTSIZECOVERAGE','TMAXLOTSIZE',
    'TMINLOTSIZE','TPRIORITY','TRANSPORTCALID','TRATIO','TROUNDING','TSHIPMENTFREQUENCY'])


    data_to_be_written_to_csv["LOCFR"]=[inputrow["from_serial"]]
    data_to_be_written_to_csv["LOCID"]=[inputrow["to_serial"]]
    #data_to_be_written_to_csv["ACTID"]=programm_descname
    data_to_be_written_to_csv["PRDID"]=[inputrow["ware"]]
    data_to_be_written_to_csv["LEADTIMETRANSPORTATION"]=[inputrow["size"]]
    data_to_be_written_to_csv["TRATIO"]=1
    data_to_be_written_to_csv.to_csv("pds_transport_lanes_timeseries.csv",sep=";",index=False,mode='a',header=not os.path.exists("pds_transport_lanes_timeseries.csv"))


def read_transportation_lanes_from_csv(directory):
  dataframes=[]
  for root,dirs,files in os.walk(directory):
      for file in files:
         if (file.endswith("transport_lanes.csv") & os.path.exists(file)):

            dataframes.append(pd.read_csv(file,sep=";",low_memory=False,names=["from_serial","from_name","ware","size","to_serial","to_name"],error_bad_lines=False))
  return dataframes



def write_init_key_figures(building_entry):
  data_to_be_written_to_csv = pd.DataFrame(columns=['KEYFIGUREDATE','PRDID','LOCID','ADDITIONALLOTSIZEDEMAND','ADJUSTEDDEMAND',
  'ADJUSTEDRECEIPT','CONFIRMEDRECEIPT','COVERAGE','DEFICIT','DEPENDENTDEMAND','EXTERNALRECEIPTCOSTRATE','FIXEDEXTERNALRECEIPTCOST',
  'INDEPDEMANDNONDELIVERYCOSTRATE','INDEPDEMDELIVEREDLATE','INDEPDEMDELIVEREDLATEAVG','INDEPDEMLATEDELIVCOSTRATE','INDEPDEMLATEDELIVMAX',
  'INDEPENDENTDEMAND','INDEPENDENTDEMANDSUPPLY','INITIALINVENTORY','INVENTORYCORRECTION','INVENTORYCORRECTIONVIOLATED','INVENTORYHOLDINGCOSTRATE',
  'INVENTORYTARGETADD','INVENTORYTARGETVIOLATIONCOSTRATE','IPUDELIVEREDLATE','IPULATEDELIVCOSTRATE','IPULATEDELIVMAX','IPURECEIPTCOSTRATE',
  'LPQUOTACOVERAGE','MAXINVENTORY','MAXINVENTORYVIOLATIONCOSTRATE','MAXRECEIPT','MAXRECEIPTQUOTA','MINRECEIPT','MINRECEIPTQUOTA',
  'NETDEMAND','PROJECTEDINVENTORY','RECEIPT','RECEIPTRATIOCOMP','SHORTAGE','SUBPERIODNUM','SUBPERIODSOFMAXCOVERAGE','SUBPERIODSOFSUPPLY',
  'SUBPERIODSOFSUPPLYSAFESTOCK','SUPPLY','TARGETPERIODCUSTDEMAND','TARGETSAFETYSTOCK','TOTALINDEPDEMANDSUPPLY','TOTALRECEIPT'])
  if building_entry["is_warehouse"]:
    if (building_entry["wares"] != "na"):
        wares = re.sub(r".*wares\:", "", building_entry["wares"])
        Dict = json.loads(wares)
        wares_amount_data = pd.DataFrame.from_dict(Dict.items())
        wares_amount_data.columns = ['wares', 'amount']
        data_to_be_written_to_csv["PRDID"]=wares_amount_data["wares"]
        data_to_be_written_to_csv["INITIALINVENTORY"]=wares_amount_data["amount"]
    else: return
    data_to_be_written_to_csv['LOCID']=building_entry["serial"]
    data_to_be_written_to_csv["KEYFIGUREDATE"]=building_entry["time"]
    data_to_be_written_to_csv.to_csv("pds_init_key_figures_timeseries.csv",sep=";",index=False,mode='a',header=not os.path.exists("pds_init_key_figures_timeseries.csv"))
  if building_entry["is_production_site"]:

    if (len(building_entry["input_queues"])<3):
      return
    wares=building_entry["input_queues"]
    Dict = json.loads(wares)
    wares_amount_data = pd.DataFrame.from_dict(Dict.items())
    wares_amount_data.columns = ['wares', 'amount']
    data_to_be_written_to_csv["PRDID"]=wares_amount_data["wares"]
    data_to_be_written_to_csv["INITIALINVENTORY"]=wares_amount_data["amount"].apply(json_and_index,args=[2])
    data_to_be_written_to_csv['LOCID']=building_entry["serial"]
    data_to_be_written_to_csv["KEYFIGUREDATE"]=building_entry["time"]
    data_to_be_written_to_csv.to_csv("pds_init_key_figures_timeseries.csv",sep=";",index=False,mode='a',header=not os.path.exists("pds_init_key_figures_timeseries.csv"))
 


def write_demand_key_figures(building_entry):

  
  data_to_be_written_to_csv = pd.DataFrame(columns=['KEYFIGUREDATE','PRDID','LOCID','ADDITIONALLOTSIZEDEMAND',
  'ADJUSTEDDEMAND','ADJUSTEDRECEIPT','CONFIRMEDRECEIPT','COVERAGE','DEFICIT','DEPENDENTDEMAND','EXTERNALRECEIPTCOSTRATE',
  'FIXEDEXTERNALRECEIPTCOST','INDEPDEMANDNONDELIVERYCOSTRATE','INDEPDEMDELIVEREDLATE','INDEPDEMDELIVEREDLATEAVG',
  'INDEPDEMLATEDELIVCOSTRATE','INDEPDEMLATEDELIVMAX','INDEPENDENTDEMAND','INDEPENDENTDEMANDSUPPLY',
  'INITIALINVENTORY','INVENTORYCORRECTION','INVENTORYCORRECTIONVIOLATED','INVENTORYHOLDINGCOSTRATE',
  'INVENTORYTARGETADD','INVENTORYTARGETVIOLATIONCOSTRATE','IPUDELIVEREDLATE','IPULATEDELIVCOSTRATE',
  'IPULATEDELIVMAX','IPURECEIPTCOSTRATE','LPQUOTACOVERAGE','MAXINVENTORY','MAXINVENTORYVIOLATIONCOSTRATE',
  'MAXRECEIPT','MAXRECEIPTQUOTA','MINRECEIPT','MINRECEIPTQUOTA','NETDEMAND','PROJECTEDINVENTORY','RECEIPT',
  'RECEIPTRATIOCOMP','SHORTAGE','SUBPERIODNUM','SUBPERIODSOFMAXCOVERAGE','SUBPERIODSOFSUPPLY','SUBPERIODSOFSUPPLYSAFESTOCK','SUPPLY','TARGETPERIODCUSTDEMAND',
  'TARGETSAFETYSTOCK','TOTALINDEPDEMANDSUPPLY','TOTALRECEIPT'])


  #IBPproduct_location["MNG01"]=wares_amount_data["amount"]
  data_to_be_written_to_csv['LOCID']=[building_entry["serial"]]
  data_to_be_written_to_csv["PRDID"]=building_entry["requested_Ware"]
  data_to_be_written_to_csv["KEYFIGUREDATE"]=building_entry["time"]
  data_to_be_written_to_csv["INDEPENDENTDEMAND"]=building_entry["amount"]

    

  
    #IBPproduct_location["VRFKZ"]="X"
  data_to_be_written_to_csv.to_csv("pds_demand_key_figures_timeseries.csv",sep=";",index=False,mode='a',header=not os.path.exists("pds_demand_key_figures_timeseries.csv"))


def read_transfer_transportation_lanes_from_csv_new(directory):
  dataframes=[]
  for root,dirs,files in os.walk(directory):
      for file in files:
         if (file.endswith("transfer_tl.csv") & os.path.exists(file)):

            dataframes.append(pd.read_csv(file,sep=";",low_memory=False,names=["from_serial","from_name","ware","size","to_serial","to_name"],error_bad_lines=False))
  return dataframes

def read_independent_demand_from_csv_new(directory):
  dataframes=[]
  for root,dirs,files in os.walk(directory):
      for file in files:
         if (file.endswith("independent_demand.csv") & os.path.exists(file)):

            dataframes.append(pd.read_csv(file,sep=";",low_memory=False,names=["serial","name","time","wares"],error_bad_lines=False))
  return dataframes


def write_ind_demand_key_figures(building_entry):

  
  data_to_be_written_to_csv = pd.DataFrame(columns=['KEYFIGUREDATE','PRDID','LOCID','ADDITIONALLOTSIZEDEMAND',
  'ADJUSTEDDEMAND','ADJUSTEDRECEIPT','CONFIRMEDRECEIPT','COVERAGE','DEFICIT','DEPENDENTDEMAND','EXTERNALRECEIPTCOSTRATE',
  'FIXEDEXTERNALRECEIPTCOST','INDEPDEMANDNONDELIVERYCOSTRATE','INDEPDEMDELIVEREDLATE','INDEPDEMDELIVEREDLATEAVG',
  'INDEPDEMLATEDELIVCOSTRATE','INDEPDEMLATEDELIVMAX','INDEPENDENTDEMAND','INDEPENDENTDEMANDSUPPLY',
  'INITIALINVENTORY','INVENTORYCORRECTION','INVENTORYCORRECTIONVIOLATED','INVENTORYHOLDINGCOSTRATE',
  'INVENTORYTARGETADD','INVENTORYTARGETVIOLATIONCOSTRATE','IPUDELIVEREDLATE','IPULATEDELIVCOSTRATE',
  'IPULATEDELIVMAX','IPURECEIPTCOSTRATE','LPQUOTACOVERAGE','MAXINVENTORY','MAXINVENTORYVIOLATIONCOSTRATE',
  'MAXRECEIPT','MAXRECEIPTQUOTA','MINRECEIPT','MINRECEIPTQUOTA','NETDEMAND','PROJECTEDINVENTORY','RECEIPT',
  'RECEIPTRATIOCOMP','SHORTAGE','SUBPERIODNUM','SUBPERIODSOFMAXCOVERAGE','SUBPERIODSOFSUPPLY','SUBPERIODSOFSUPPLYSAFESTOCK','SUPPLY','TARGETPERIODCUSTDEMAND',
  'TARGETSAFETYSTOCK','TOTALINDEPDEMANDSUPPLY','TOTALRECEIPT'])

  Dict = json.loads(building_entry["wares"])
  wares_amount_data = pd.DataFrame.from_dict(Dict.items())
  wares_amount_data.columns = ['wares', 'amount']
  data_to_be_written_to_csv["PRDID"]=wares_amount_data["wares"]
  data_to_be_written_to_csv["INDEPENDENTDEMAND"]=wares_amount_data["amount"]


  data_to_be_written_to_csv['LOCID']=building_entry["serial"]

  data_to_be_written_to_csv["KEYFIGUREDATE"]=building_entry["time"]


    

  
    #IBPproduct_location["VRFKZ"]="X"
  data_to_be_written_to_csv.to_csv("pds_demand_key_figures_timeseries.csv",sep=";",index=False,mode='a',header=not os.path.exists("pds_demand_key_figures_timeseries.csv"))



def write_all_data(directory,tribe):

  transfer_tl = read_transfer_transportation_lanes_from_csv_new(directory)
  Trans_lanes = read_transportation_lanes_from_csv(directory)
  independent_demand=read_independent_demand_from_csv_new(directory)
  
  a= read_data_from_files(directory)
  
  for i in range(len(a)) :
    headquarter_index=a[i][a[i]["name"].str.endswith("headquarters")].index[0]
    if(a[i].loc[headquarter_index,"name"].startswith(tribe)):
      data_index=i
  production_programs=read_production_programms_data_from_files(directory)
  for i in range(len(production_programs)):
    if (production_programs[i].loc[0,"name"].startswith(tribe)):
      production_programs_index=i


  for i in range(len(transfer_tl)):
    headquarter_index=transfer_tl[i][transfer_tl[i]["from_name"]!= "constructionsite"].index[0]
    if (transfer_tl[i].loc[headquarter_index,"from_name"].startswith(tribe)):
      t_l_index=i
  trans_lanes_empty=False
  for i in range(len(Trans_lanes)):
    try:
      headquarter_index=Trans_lanes[i][Trans_lanes[i]["from_name"] != "constructionsite"].index[0]
    except Exception:
      trans_lanes_empty=True

    
    if (not trans_lanes_empty):
      if (Trans_lanes[i].loc[headquarter_index,"from_name"].startswith(tribe)) :
          Trans_lanes_index=i

  for i in range(len(independent_demand)):
    if (independent_demand[i].loc[0,"name"].startswith(tribe)):
      independent_demand_index=i
  if (not trans_lanes_empty): 
    transfer_tl = transfer_tl[t_l_index].append(Trans_lanes[Trans_lanes_index])

  a=map_to_real_time(a,data_index)
  independent_demand=map_to_real_time(independent_demand,independent_demand_index)
  
  if (trans_lanes_empty):
     transfer_tl=transfer_tl[t_l_index][transfer_tl[t_l_index]["to_name"]!="constructionsite"]
  elif (not trans_lanes_empty):
    transfer_tl=transfer_tl[transfer_tl["to_name"]!="constructionsite"]
  
  transfer_tl=transfer_tl[transfer_tl["from_name"]!="constructionsite"]
  
  #transfer_tl.drop_duplicates(["ware","from_serial","to_serial"]).apply(lambda inputrow:write_pds_transport_lanes(inputrow),axis=1)
  #del transfer_tl
  
  headquarter_index=a[data_index][a[data_index]["name"].str.endswith("headquarters")].index[0]
  
  products=write_singel_string_wares_attribute_to_csv_as_IBPproduct(a[data_index].iloc[headquarter_index]["wares"])
  products.to_csv("PRODUCT_timeseries.csv",sep=";",index=False,header=True)
  del products
  write_series_of_location_to_csv_as_IBPlocation(a[data_index][["serial","name"]].drop_duplicates(subset="serial"))
  production_programs[production_programs_index].apply(lambda x: write_pds_comp(x["serial"],x["produced_Wares"],x["consumed_wares_workers"],x["programm_descname"]),axis=1)
  temp = pd.read_csv("pds_comp_timeseries.csv",sep=";")
  temp=temp.drop_duplicates()
  temp.to_csv("pds_comp_timeseries.csv",sep=";",header=True,index=False)
  a[data_index][["serial","wares","input_queues","is_warehouse","is_production_site"]].drop_duplicates("serial").apply(lambda x: write_product_location(x["serial"],x["wares"],x["input_queues"],x.is_warehouse,x.is_production_site),axis=1)
  production_programs[production_programs_index].apply(lambda inputrow:write_product_location_from_production_programs(inputrow),axis=1)

  production_programs[production_programs_index].apply(lambda inputrow:write_pds_header_additionals(inputrow),axis=1)


  #drop duplicates
  temp = pd.read_csv("pds_header_additionals_timeseries.csv",sep=";")
  temp.drop_duplicates().to_csv("pds_header_additionals_timeseries.csv",sep=";",header=True,index=False)

  

  #take only the tls tht are in pds_header or produkts coming and going from warehouses
  transfer_tl_new = transfer_tl[transfer_tl.set_index(['ware','from_serial']).index.isin(temp.set_index(['PRDID','LOCID']).index)].drop_duplicates()
  fucklist=list(a[data_index].loc[a[data_index]["is_warehouse"]==1].drop_duplicates(subset="serial")["serial"])
  transfer_tl=transfer_tl[transfer_tl["from_serial"].isin(fucklist)]
  transfer_tl=transfer_tl.append(transfer_tl_new)
  transfer_tl.drop_duplicates(["ware","from_serial","to_serial"]).apply(lambda inputrow:write_pds_transport_lanes(inputrow),axis=1)
  del Trans_lanes
  del transfer_tl
  del transfer_tl_new
  del fucklist

  
  
  temp = pd.read_csv("product_location_timeseries.csv",sep=";")
  temp=temp.drop_duplicates()
  temp.to_csv("product_location_timeseries.csv",sep=";",header=True,index=False)

  last_headquarter_index=a[data_index][a[data_index]["name"].str.endswith("headquarters")].index[0]  


  rounded = a[data_index].copy(deep=True)
  rounded.loc[:, 'time'] = rounded.time.apply(datetime.datetime.date)
  rounded.loc[:, 'time'] = pd.to_datetime(rounded.time)+pd.offsets.MonthBegin(0)
  write_init_key_figures(rounded.iloc[last_headquarter_index])
  try:
    last_warehouse_index=a[data_index][a[data_index]["name"].str.endswith("warehouse")].index[0]  
    rounded = a[data_index].copy(deep=True)
    rounded.loc[:, 'time'] = rounded.time.apply(datetime.datetime.date)
    rounded.loc[:, 'time'] = pd.to_datetime(rounded.time)+pd.offsets.MonthBegin(0)
    write_init_key_figures(rounded.iloc[last_warehouse_index])
  except:Exception

  rounded = independent_demand[independent_demand_index].copy(deep=True)
  rounded.loc[:, 'time'] = rounded.time.apply(datetime.datetime.date)
  rounded.loc[:, 'time'] = pd.to_datetime(rounded.time)+pd.offsets.MonthBegin(0)
  rounded.drop_duplicates("time").apply(lambda x: write_ind_demand_key_figures(x),axis=1)


#  tribes  : 
#  amazons , atlanteans , barbarians ,empire ,frisians

#write_all_data(os.getcwd() ,"empire")