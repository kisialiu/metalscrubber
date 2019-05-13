import grequests
import json
from bs4 import BeautifulSoup
from pymongo import MongoClient

discs = "https://www.metal-archives.com/band/discography/id/{}/tab/all"
client = MongoClient()
db = client.on_metal_db
bandMap = {}

def getDiscs(resultSet):
    list = []
    for tr in resultSet:
        info = tr.find_all('td')
        disc = {
            "_id": info[0].find('a')['href'].split("/")[6],
            "name": info[0].find('a').text,
            "type": info[1].text,
            "year": info[2].text
        }
        list.append(disc)
    return list

def getDiscography(response, *args, **kwargs):
    id = response.url.split("/")[6]
    bandMap[id] = getDiscs(BeautifulSoup(response.text, "html.parser").find('tbody').find_all('tr'))

cursor = db.bands.find({'discography.0._id': {'$type': 6}})
urls = []
for band in cursor:
    bandId = band["_id"]
    urls.append(discs.format(bandId))
rs = (grequests.get(u, hooks={'response': getDiscography}, verify=False) for u in urls)
grequests.map(rs)

for id in bandMap.keys():
    db.bands.update_one(
        {"_id": id},
        {"$set": {"discography": bandMap[id]}}
    )