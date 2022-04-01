from pymongo import MongoClient
import json, csv
from options import Options
import pandas as pd

mongoUser = ''
mongoPassword = ''
mongoDB = ''

Host = 'localhost' 
Port = 27017

DSN = "mongodb://{}:{}".format(Host,Port)

conn = MongoClient(DSN)

bd = conn['MongoDB-BDNR']

try:
    coll_Patient = bd.create_collection('Patient')
except: 
    bd.Patient.drop()
    coll_Patient = bd.create_collection('Patient')
try:
    coll_Nodule= bd.create_collection('Nodule')
except:
    bd.Nodule.drop()
    coll_Nodule= bd.create_collection('Nodule')

try: 
    coll_Experiment = bd.create_collection('Experiment')
except:    
    bd.Experiment.drop()
    coll_Experiment = bd.create_collection('Experiment')
try:
    coll_CTScanner = bd.create_collection('CTScanner')
except:
    bd.CTScanner.drop()
    coll_CTScanner = bd.create_collection('CTScanner')

try:
    coll_Relation = bd.create_collection('Relation')
except:
    bd.Relation.drop()
    coll_Relation = bd.create_collection('Relation')
    
csvfile1 = pd.read_excel("Dades.xlsx", sheet_name="Cases")
csvfile2 = pd.read_excel("Dades.xlsx", sheet_name="Training")
csvfile3 = pd.read_excel("Dades.xlsx", sheet_name="MethodOutput")

headers_Relation=["PatientID","NodulID","MethodID", "ExperimentRepetition", "Train", "RadiomicsDiagnosis"]
col = {}
for at in headers_Relation:
    for col_name, data in csvfile2.items():
        if(col_name in at):
            for val in data:
                col[col_name]=data
    list_of_dicts_Relation = [dict(zip(col,t)) for t in zip(*col.values())]

for dictionary in list_of_dicts_Relation:
    coll_Relation.insert_one(dictionary)


headers_Patient=["PatientID","Age","Gender", "DiagnosisPatient"]
col = {}
for at in headers_Patient:
    for col_name, data in csvfile1.items():
        if(col_name in at):
            for val in data:
                col[col_name]=data
    list_of_dicts = [dict(zip(col,t)) for t in zip(*col.values())]
id_added=[]
for dictionary in list_of_dicts:
    dictionary["_id"]=dictionary["PatientID"]
    dictionary.pop("PatientID")
    if(dictionary["_id"] not in id_added):
        coll_Patient.insert_one(dictionary)
        id_added.append(dictionary["_id"])

headers_CTScanner=["CTID","Device","dataCT","ResolutionTC","ResolutionTV","ResolutionT","Diameter (mm)"]
col = {}
for at in headers_CTScanner:
    for col_name, data in csvfile1.items():
        if(col_name in at):
            for val in data:
                col[col_name]=data
    list_of_dicts_CTScanner = [dict(zip(col,t)) for t in zip(*col.values())]
for dictionary in list_of_dicts_CTScanner:
    coll_CTScanner.insert_one(dictionary)

headers_ScannerEncast=["CTID", "Device", "dataCT","Diameter (mm)"]
col = {}
for at in headers_ScannerEncast:
    for col_name, data in csvfile1.items():
        if(col_name in at):
            for val in data:
                col[col_name]=data
    list_of_dicts_ScannerEncast = [dict(zip(col,t)) for t in zip(*col.values())]
    
headers_Nodule=["PatientID","NoduleID","DiagnosisPatient","DiagnosisNodule","PositionX","PositionY","PositionZ"]
col = {}
for at in headers_Nodule:
    for col_name, data in csvfile1.items():
        if(col_name in at):
            for val in data:
                col[col_name]=data
    list_of_dicts = [dict(zip(col,t)) for t in zip(*col.values())]
for dictionary1,dictionary2 in zip(list_of_dicts,list_of_dicts_ScannerEncast):
    dictionary1["CTScanner"]=dictionary2
    coll_Nodule.insert_one(dictionary1)


headers_Method=["MethodID","FeatSelection","FeatDescription","Classifier"]
col = {}
for at in headers_Method:
    for col_name, data in csvfile3.items():
        if(col_name in at):
            for val in data:
                col[col_name]=data
    list_of_dicts_Method = [dict(zip(col,t)) for t in zip(*col.values())]
headers_Experiment=["Train","BenignPrec","BenignRec","MalignPrec","MalignRec","Repetition"]
col = {}
for at in headers_Experiment:
    for col_name, data in csvfile3.items():
        if(col_name in at):
            for val in data:
                col[col_name]=data
    list_of_dicts_Experiment = [dict(zip(col,t)) for t in zip(*col.values())]
headers_NoduleEncast=["NoduleID","DiagnosisNodule"]
col = {}
for at in headers_NoduleEncast:
    for col_name, data in csvfile1.items():
        if(col_name in at):
            for val in data:
                col[col_name]=data
    list_of_dicts_NoduleEncast = [dict(zip(col,t)) for t in zip(*col.values())]
for dictionary1,dictionary2, dictionary3 in zip(list_of_dicts_Experiment,list_of_dicts_Method, list_of_dicts_NoduleEncast):
    dictionary1["Method"]=dictionary2
    dictionary1["Nodule"]=dictionary3
    coll_Experiment.insert_one(dictionary1)

