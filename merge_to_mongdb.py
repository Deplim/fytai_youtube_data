import json
from pymongo import MongoClient

file=open("merge_data.json", encoding='UTF8')
merge_data=json.load(file)

connector = MongoClient('localhost', 27017)
db = connector['fytai']
col = db["video_test"]

for i in range(len(merge_data)):
    del merge_data[i]["_id"]

x = col.insert_many(merge_data)