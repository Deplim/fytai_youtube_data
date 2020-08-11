from youtube_transcript_api import YouTubeTranscriptApi
import requests
import hashlib
from datetime import datetime
from pymongo import MongoClient
from tqdm import tqdm

def get_hashed(title, date):
   t = title + datetime.strftime(date, "%Y-%m-%d")
   print(t, "\n")
   return hashlib.md5(t.encode('utf-8')).hexdigest()

def storage_video_info(storage):
    connector = MongoClient('localhost', 27017)
    db = connector['fytai']
    col = db["video_test"]
    x = col.insert_many(storage)
    return True


def get_playlist_form_txt(target):
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


def get_video_list(target):
    video_list=[]

    request_url = "https://www.googleapis.com/youtube/v3/playlistItems?key=AIzaSyBs-ft82koBxP1FyPvVbjPW6Z74gICSHS0&part=snippet&playlistId="+target+"&maxResults=50"
    #print(request_url)
    comments_data = requests.get(request_url)
    jsonized_data = comments_data.json()
    for i in jsonized_data["items"]:
        video_list.append(i["snippet"]["resourceId"]["videoId"])

    if "nextPageToken" in jsonized_data:
        nextPageToken = jsonized_data["nextPageToken"]

        request_url = "https://www.googleapis.com/youtube/v3/playlistItems?key=AIzaSyBs-ft82koBxP1FyPvVbjPW6Z74gICSHS0&part=snippet&playlistId="+target+"&maxResults=50&pageToken="+nextPageToken
        #print(request_url)
        comments_data = requests.get(request_url)
        jsonized_data = comments_data.json()
        for i in jsonized_data["items"]:
            video_list.append(i["snippet"]["resourceId"]["videoId"])
    return video_list


def get_comment_list(target_list):
    comment_list=[]
    total = tqdm(total=len(target_list))

    for i, v in enumerate(target_list):
        #print("i : ", i)
        temp_list=[]

        request_url = "https://www.googleapis.com/youtube/v3/commentThreads?key=AIzaSyBs-ft82koBxP1FyPvVbjPW6Z74gICSHS0&textFormat=plainText&part=snippet,replies&order=relevance&videoId="+v+"&maxResults=100"
        #print(request_url)
        comments_data = requests.get(request_url)
        jsonized_data = comments_data.json()
        for j in jsonized_data["items"]:
            temp={}
            temp["content"]=j["snippet"]["topLevelComment"]["snippet"]["textOriginal"].replace("\n", "")
            temp["date"]=datetime.strptime(j["snippet"]["topLevelComment"]["snippet"]["updatedAt"], '%Y-%m-%dT%H:%M:%SZ')
            temp["like"]=j["snippet"]["topLevelComment"]["snippet"]["likeCount"]
            temp["replies"]=[]
            if "replies" in j:
                for m in j["replies"]["comments"]:
                    rp_temp={}
                    rp_temp["content"]=m["snippet"]["textOriginal"]
                    rp_temp["date"]=datetime.strptime(m["snippet"]["updatedAt"], '%Y-%m-%dT%H:%M:%SZ')
                    rp_temp["like"]=m["snippet"]["likeCount"]
                    temp["replies"].append(rp_temp)
            temp_list.append(temp)

        while "nextPageToken" in jsonized_data:
            nextPageToken=jsonized_data["nextPageToken"]

            request_url = "https://www.googleapis.com/youtube/v3/commentThreads?key=AIzaSyBs-ft82koBxP1FyPvVbjPW6Z74gICSHS0&textFormat=plainText&part=snippet,replies&order=relevance&videoId=" + v + "&maxResults=100&pageToken="+nextPageToken
            #print(request_url)
            comments_data = requests.get(request_url)
            jsonized_data = comments_data.json()
            for j in jsonized_data["items"]:
                temp = {}
                temp["content"] = j["snippet"]["topLevelComment"]["snippet"]["textOriginal"].replace("\n", "")
                temp["date"] = datetime.strptime(j["snippet"]["topLevelComment"]["snippet"]["updatedAt"], '%Y-%m-%dT%H:%M:%SZ')
                temp["like"] = j["snippet"]["topLevelComment"]["snippet"]["likeCount"]
                temp["replies"] = []
                if "replies" in j:
                    for m in j["replies"]["comments"]:
                        rp_temp = {}
                        rp_temp["content"] = m["snippet"]["textOriginal"]
                        rp_temp["date"] = datetime.strptime(m["snippet"]["updatedAt"], '%Y-%m-%dT%H:%M:%SZ')
                        rp_temp["like"] = m["snippet"]["likeCount"]
                        temp["replies"].append(rp_temp)
                temp_list.append(temp)

        comment_list.append(temp_list)
        #print("\n-----------------------------------\n\n\n")
        total.update(1)
    return comment_list


def get_video_info_list(target_list):
    info_list = []

    for i, v in enumerate(target_list):
        #print("i : ", i)
        temp = {}

        request_url = "https://www.googleapis.com/youtube/v3/videos?key=AIzaSyBs-ft82koBxP1FyPvVbjPW6Z74gICSHS0&part=snippet,statistics&id=" + v
        #print(request_url)
        comments_data = requests.get(request_url)
        jsonized_data = comments_data.json()
        snippet=jsonized_data["items"][0]["snippet"]
        statistics=jsonized_data["items"][0]["statistics"]
        temp["channel"]=None
        temp["title"]=snippet["title"]
        temp["content"]=snippet["description"]
        temp["date"]=datetime.strptime(snippet["publishedAt"], '%Y-%m-%dT%H:%M:%SZ')
        temp["view"]=int(statistics["viewCount"])
        temp["like"]=int(statistics["likeCount"])
        temp["dislike"]=int(statistics["dislikeCount"])
        temp["comments"]=None
        temp["hash"]=v

        info_list.append(temp)
        #print("\n-----------------------------------\n\n\n")
    return info_list


if __name__ == '__main__':
    #print(get_hashed("1분과학", datetime.strptime("2016. 3. 20.", '%Y. %m. %d.')))

    playlist=get_playlist_form_txt("playlist1.txt")
    print("playlist length : "+str(len(playlist)), "\n")

    for i in playlist[:5]:
        channel_hash=get_hashed(i["name"], datetime.strptime(i["regist"], '%Y. %m. %d.'))

        video_list=get_video_list(i["playlist_id"])
        print("video list length : "+str(len(video_list)))

        video_info_list=get_video_info_list(video_list[:3])
        print("get video info list : complet\n")

        comment_list = get_comment_list(video_list[:3])
        print("comment count for video list : ")
        print([len(k) for k in comment_list], "\n")

        for j,v in enumerate(video_info_list):
            video_info_list[j]["comments"]=comment_list[j]
            video_info_list[j]["channel"]=channel_hash

        if storage_video_info(video_info_list):
            print("mongo db insert: video info list >> success !!")

        print("\n\n-------------------------\n\n")

