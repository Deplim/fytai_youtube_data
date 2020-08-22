import requests
import hashlib
from datetime import datetime
from pymongo import MongoClient
from tqdm import tqdm

def get_hashed(title, date):
   t = title + datetime.strftime(date, "%Y-%m-%d")
   print(t, end=" : ")
   return hashlib.md5(t.encode('utf-8')).hexdigest()

def get_playlist_from_txt(target):
    channel_list=[]

    file=open(target, encoding='UTF8')
    text=(file.read()).split("\n\n")

    for i in text:
        temp_dict = {}
        if len(i)<5:
            continue

        temp=i.split("\n")

        temp_dict["name"]=temp[0]
        temp_dict["regist"]=temp[1]
        temp_dict["playlist_id"] = temp[2]
        channel_list.append(temp_dict)
    return channel_list

def storage_to_db(hash):
    connector = MongoClient('localhost', 27017)
    db = connector['fytai']
    col = db["video_test"]


    col.update_many({"channel": hash}, {"$set": {"class": "science"}}, upsert=True)

if __name__ == '__main__':
    print("get playlist from txt >>> ", end="")
    playlist = get_playlist_from_txt("science.txt")
    print("playlist length : " + str(len(playlist)), "\n")

    for i in playlist:
        channel_hash=get_hashed(i["name"], datetime.strptime(i["regist"], '%Y. %m. %d.'))
        storage_to_db(channel_hash)
