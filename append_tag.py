from pymongo import MongoClient
from tqdm import tqdm
import requests
import json

def get_target():
    connector = MongoClient('localhost', 27017)
    db = connector['fytai']
    col = db["video_test"]
    x = col.find()
    #print(x[0])
    return x

def get_tag(key):
    target=get_target()
    target=[i["hash"] for i in target if "hash" in i and len(i["comments"])!=0]

    tag_list = []
    total = tqdm(total=len(target))

    for v in target:
        try:
            request_url = "https://www.googleapis.com/youtube/v3/videos?key=" + key + "&part=snippet&id=" + v
            comments_data = requests.get(request_url)
            jsonized_data = comments_data.json()
            tags=jsonized_data["items"][0]["snippet"]["tags"]

            tag_list.append({"hash":v, "tags":tags})
            total.update(1)
        except Exception as e:
            print(e)
            print(v, " : ", request_url)

            tag_list.append({"hash": v, "tags": []})
            total.update(1)

    storage_to_db(tag_list)

    with open("tags.json", "w", encoding='UTF8') as file:
        json.dump(tag_list, file, ensure_ascii=False)


def storage_to_db(tag_list):
    print("0")
    connector = MongoClient('localhost', 27017)
    db = connector['fytai']
    col = db["video_test"]

    for v in tag_list:
        col.update_one({"hash":v["hash"]}, {"$set":{"tags":v["tags"]}}, upsert=True)

#get_target()
get_tag("AIzaSyBiWjDYSlhHltHOr-L83Iocz7kf74t3myA")