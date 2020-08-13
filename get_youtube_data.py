from youtube_transcript_api import YouTubeTranscriptApi
import requests
import hashlib
from datetime import datetime
from pymongo import MongoClient
from tqdm import tqdm

def get_hashed(title, date):
   t = title + datetime.strftime(date, "%Y-%m-%d")
   print(t, end=" : ")
   return hashlib.md5(t.encode('utf-8')).hexdigest()

def storage_video_info(storage):
    connector = MongoClient('localhost', 27017)
    db = connector['fytai']
    col = db["video_test"]
    x = col.insert_many(storage)
    return True


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


def get_video_list(target, key):
    video_list=[]

    request_url = "https://www.googleapis.com/youtube/v3/playlistItems?key="+key+"&part=snippet&playlistId="+target+"&maxResults=50"
    print(request_url)
    comments_data = requests.get(request_url)
    jsonized_data = comments_data.json()
    for i in jsonized_data["items"]:
        video_list.append(i["snippet"]["resourceId"]["videoId"])

    if "nextPageToken" in jsonized_data:
        nextPageToken = jsonized_data["nextPageToken"]

        request_url = "https://www.googleapis.com/youtube/v3/playlistItems?key="+key+"&part=snippet&playlistId="+target+"&maxResults=50&pageToken="+nextPageToken
        #print(request_url)
        comments_data = requests.get(request_url)
        jsonized_data = comments_data.json()
        for i in jsonized_data["items"]:
            video_list.append(i["snippet"]["resourceId"]["videoId"])
    return video_list


def get_comment_list(target_list, key):
    comment_list=[]
    total = tqdm(total=len(target_list))

    for i, v in enumerate(target_list):
        #print("i : ", i)
        current_url=""
        message="0"
        try:
            temp_list=[]
            current_url=request_url = "https://www.googleapis.com/youtube/v3/commentThreads?key="+key+"&textFormat=plainText&part=snippet,replies&order=relevance&videoId="+v+"&maxResults=100"
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

            message="1"

            while "nextPageToken" in jsonized_data:
                nextPageToken=jsonized_data["nextPageToken"]

                current_url=request_url = "https://www.googleapis.com/youtube/v3/commentThreads?key="+key+"&textFormat=plainText&part=snippet,replies&order=relevance&videoId=" + v + "&maxResults=100&pageToken="+nextPageToken
                message = "2"
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
                message = "3"

            comment_list.append(temp_list)
            #print("\n-----------------------------------\n\n\n")
            total.update(1)
        except Exception as e:
            print("get_comment_error:",e)
            print(current_url)
            print(message)

            comment_list.append([])
            total.update(1)
    return comment_list


def get_video_info_list(target_list, key):
    info_list = []

    for i, v in enumerate(target_list):
        #print("i : ", i)
        current_url=""

        try:
            temp = {}
            current_url=request_url = "https://www.googleapis.com/youtube/v3/videos?key=" + key + "&part=snippet,statistics&id=" + v
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
        except Exception as e:
            print("get_video_error : ",e)
            print(current_url)

            temp["channel"] = None
            temp["title"]=0
            temp["comments"]=None
            info_list.append(temp)
    return info_list


if __name__ == '__main__':
    #print(get_hashed("1분과학", datetime.strptime("2016. 3. 20.", '%Y. %m. %d.')))

    print("get playlist from txt >>> ", end="")
    playlist=get_playlist_from_txt("playlist3_copy.txt")
    print("playlist length : "+str(len(playlist)), "\n")

    error_list=[]
    key="AIzaSyBlFCIyih5ru1iaY6vzIJgdLgBpIjHIw64"

    print("start !!! \n===============================\n===============================\n\n")

    for i in playlist:
        try:
            channel_hash=get_hashed(i["name"], datetime.strptime(i["regist"], '%Y. %m. %d.'))
            print(channel_hash, "\n")

            print("get video list >>> ")
            video_list=get_video_list(i["playlist_id"], key)
            print("video list length : "+str(len(video_list)), "\n")

            print("get info list >>> ")
            video_info_list=get_video_info_list(video_list, key)
            print("title for video list : ")
            check1=[k["title"] for k in video_info_list]
            print(check1, "\n")

            print("get comment list >>> ")
            comment_list = get_comment_list(video_list, key)
            print("comment count for video list : ")
            check2=[len(k) for k in comment_list]
            print(check2, "\n")

            if check1.count(0)>5 or check2.count([])>5:
                t=input("확인 필요")

            for j,v in enumerate(video_info_list):
                video_info_list[j]["comments"]=comment_list[j]
                video_info_list[j]["channel"]=channel_hash

            if storage_video_info(video_info_list):
                print("mongo db insert: video info list >> success !!")

            print("\n\n-------------------------\n\n")
        except Exception as e:
            print("channel error : ", e)
            error_list.append(i)

    print("error:")
    print(error_list)


