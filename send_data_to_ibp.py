import generate_Data
import uploadmasterdata
import start_planning_run
import downloaddata
import os
import time
import sys
def delet_files_content():
  f = open("product_location_timeseries.csv", "w")
  f.truncate()
  f.close()
  f = open("PRODUCT_timeseries.csv", "w")
  f.truncate()
  f.close()
  f = open("pds_transport_lanes_timeseries.csv", "w")
  f.truncate()
  f.close()
  f = open("pds_init_key_figures_timeseries.csv", "w")
  f.truncate()
  f.close()
  f = open("pds_header_additionals_timeseries.csv", "w")
  f.truncate()
  f.close()
  f = open("pds_demand_key_figures_timeseries.csv", "w")
  f.truncate()
  f.close()
  f = open("pds_comp_timeseries.csv", "w")
  f.truncate()
  f.close()
  f = open("locations_timeseries.csv", "w")
  f.truncate()
  f.close()


#  tribes  : 
#  amazons , atlanteans , barbarians ,empire ,frisians
#if (int(sys.argv[1])<9):
generate_Data.write_all_data(directory= os.getcwd() ,tribe="empire")
uploadmasterdata.upload_all_data()
time.sleep(80)
start_planning_run.wait_until_plan_is_ready(start_planning_run.start_planning_run())
downloaddata.download_dep_Demand()
os.system("python3 delete_master_data.py")
os.system("find . -name '*timeseries.csv' -exec rm {} \;")

