import requests
import json

def planning_run():
  with open('auth.txt') as f:
    auth = [tuple(map(str, i.split(','))) for i in f]
  url = "https://o22-001-api.devsys.net.sap/sap/opu/odata/sap/BC_EXT_APPJOB_MANAGEMENT;v=2/JobSchedule?JobTemplateName='YY1_ZMBC33DU4C4PN3HEXWMJGCAZE3U'&JobText='ourtestjob'&JobUser='ADMINISTRATOR'"
  myobj = {
  }
  s = requests.Session()
  r = s.head(url,auth=auth[0], headers={'x-csrf-token': 'fetch'},verify=False)
  token = r.headers.get('x-csrf-token','')
  #print(token)
  s.headers.update({'x-csrf-token': token})
  with open('auth.txt') as f:
    auth = [tuple(map(str, i.split(','))) for i in f]
  x = s.post(url, auth=auth[0], verify=False)

def start_planning_run():
  with open('auth.txt') as f:
    auth = [tuple(map(str, i.split(','))) for i in f]
  url = "https://o22-001-api.devsys.net.sap/sap/opu/odata/sap/BC_EXT_APPJOB_MANAGEMENT;v=2/JobSchedule?JobTemplateName='YY1_ZMBC33DU4C4PN3HEXWMJGCAZE3U'&JobText='ourtestjob'&JobUser='ADMINISTRATOR'"
  myobj = {
  }
  s = requests.Session()
  s.headers.update({"Accept": "application/json"})
  r = s.head(url,auth=auth[0], headers={'x-csrf-token': 'fetch'},verify=False)
  token = r.headers.get('x-csrf-token','')
  #print(token)
  s.headers.update({'x-csrf-token': token})
  with open('auth.txt') as f:
    auth = [tuple(map(str, i.split(','))) for i in f]
  x = s.post(url, auth=auth[0], verify=False)
  return json.loads(x.text)

def wait_until_plan_is_ready(a):
  with open('auth.txt') as f:
    auth = [tuple(map(str, i.split(','))) for i in f]
  while(a["d"]["JobStatus"]!="F"):
    url="https://o22-001-api.devsys.net.sap/sap/opu/odata/sap/BC_EXT_APPJOB_MANAGEMENT;v=2/JobStatusGet?JobName='" + str(a["d"]["JobName"]) + "'&JobRunCount='" + str(a["d"]["JobRunCount"]+"'")

    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    r = s.head(url,auth=auth[0], headers={'x-csrf-token': 'fetch'},verify=False)
    token = r.headers.get('x-csrf-token','')
    #print(token)
    s.headers.update({'x-csrf-token': token})
    with open('auth.txt') as f:
      auth = [tuple(map(str, i.split(','))) for i in f]
    x = s.get(url, auth=auth[0], verify=False)
    a=json.loads(x.text)
    print(a["d"]["JobStatus"])
#planning_run()