from pymongo import MongoClient
import json, csv
from options import Options
import pandas as pd

mongoUser = ''
mongoPassword = ''
mongoDB = ''

# En execució remota
Host = 'localhost' 
Port = 27017

DSN = "mongodb://{}:{}".format(Host,Port)

conn = MongoClient(DSN)

bd = conn['MongoDB-BDNR']

try:
    coll_Patient = bd.create_collection('Patient')
    coll_Nodule= bd.create_collection('Nodule')
    coll_Method = bd.create_collection('Method')
    coll_Experiment = bd.create_collection('Experiment')
except:
    bd.Patient.drop()
    bd.Nodule.drop()
    bd.Method.drop()
    bd.Experiment.drop()
    coll_Patient = bd.create_collection('Patient')
    coll_Nodule= bd.create_collection('Nodule')
    coll_Method = bd.create_collection('Method')
    coll_Experiment = bd.create_collection('Experiment')
    
csvfile1 = pd.read_excel("C:/Users/Bernat/Downloads/Dades.xlsx", sheet_name="Cases")
csvfile2 = pd.read_excel("C:/Users/Bernat/Downloads/Dades.xlsx", sheet_name="Training")
csvfile3 = pd.read_excel("C:/Users/Bernat/Downloads/Dades.xlsx", sheet_name="MethodOutput")

headers_Patient=["PatientID","Age","Gender"]
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

headers_Experiment=["MethodID","Train","BenignPrec","BenignRec","MalignPrec","MalignRec","Repetition"]
col = {}
for at in headers_Experiment:
    for col_name, data in csvfile3.items():
        if(col_name in at):
            for val in data:
                col[col_name]=data
    list_of_dicts_Experiment = [dict(zip(col,t)) for t in zip(*col.values())]
for dictionary in list_of_dicts_Experiment:
    coll_Experiment.insert_one(dictionary)

headers_Nodule=["PatientID","NoduleID","DiagnosisPatient","DiagnosisNodule","PositionX","PositionY","PositionZ"]
col = {}
for at in headers_Nodule:
    for col_name, data in csvfile1.items():
        if(col_name in at):
            for val in data:
                col[col_name]=data
    list_of_dicts = [dict(zip(col,t)) for t in zip(*col.values())]
for dictionary1,dictionary2 in zip(list_of_dicts,list_of_dicts_CTScanner):
    dictionary1["CTScanner"]=dictionary2
    #dictionary1["Experiment"]=dictionary3
    coll_Nodule.insert_one(dictionary1)


headers_Method=["MethodID","FeatSelection","FeatDescription","Classifier"]
col = {}
for at in headers_Method:
    for col_name, data in csvfile3.items():
        if(col_name in at):
            for val in data:
                col[col_name]=data
    list_of_dicts = [dict(zip(col,t)) for t in zip(*col.values())]
id_added=[]
for dictionary in list_of_dicts:
    dictionary["_id"]=dictionary["MethodID"]
    dictionary.pop("MethodID")
    if(dictionary["_id"] not in id_added):
        coll_Method.insert_one(dictionary)
        id_added.append(dictionary["_id"])
        
