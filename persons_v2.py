# import grequests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import urllib3
import asyncio
import concurrent.futures
import requests
import time

persons_url = "https://www.metal-archives.com/artists/*/{}"
read_more = "https://www.metal-archives.com/artist/read-more/id/{}"
client = MongoClient("mongodb://vova:yfhrjvfy1@67.205.186.196:27017/admin?authMechanism=SCRAM-SHA-1")
db = client.on_metal_db
persons_map = {}
redirect_urls = []
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
http = urllib3.PoolManager()

def get_bands(a):
    bands = []
    if a is not None:
        mb = a.find_all('div', attrs={'class': 'member_in_band'})
        for div in mb:
            band = {
                "_id": div["id"].split("_")[1],
                "name": div.find("h3").text
            }
            bands.append(band)
    return bands


def done(response):
    try:
        person_id = response.url.split("/")[5]
        soup = BeautifulSoup(response.content, "html.parser")
        member_info = soup.find('div', attrs={'id': 'member_info'})
        left = member_info.find_all('dl')[0].find_all('dd')
        right = member_info.find_all('dl')[1].find_all('dd')
        member_img = soup.find('div', attrs={'class': 'member_img'})

        active = soup.find('div', attrs={'id': 'artist_tab_active'})
        past = soup.find('div', attrs={'id': 'artist_tab_past'})
        guest = soup.find('div', attrs={'id': 'artist_tab_guest'})

        person_object = {
            "name": member_info.find('h1', attrs={'class': 'band_member_name'}).text,
            "_id": person_id,
            "details": {
                "bio": "",
                "photo": member_img.find('img')['src'] if member_img is not None else "",
                "active": get_bands(active),
                "past": get_bands(past),
                "guest": get_bands(guest),
                "realName": left[0].text,
                "age": left[1].text,
                "placeOfOrigin": right[0].text,
                "gender": right[1].text
            }
        }
        db.persons.insert_one(person_object)
        print("Added person: " + person_id)
    except AttributeError:
        print(response)


def get_bio(response):
    person_id = response.url.split("/")[6]
    db.persons.update_one(
        {"_id": person_id},
        {"$set": {"details.bio": BeautifulSoup(response.content, "html.parser").text}}
    )

def get(u):
    page = ''
    # while page == '':
    try:
        page = requests.get(u)
    except:
            # print("Connection refused by the server..")
            # print("Let me sleep for 5 seconds")
            # print("ZZzzzz...")
            # time.sleep(5)
            # print("Was a nice sleep, now let me continue...")
            # continue
        print("EXCEPTION WITH:" + u)
    return page

async def aaa(urls):

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                executor,
                get,
                u
            )
            for u in urls
        ]
        for response in await asyncio.gather(*futures):
            done(response)


async def bbb(urls):

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                executor,
                requests.get,
                u
            )
            for u in urls
        ]
        for response in await asyncio.gather(*futures):
            get_bio(response)


personsIds = []
lineups = db.bands.find({}, {"currentLineup._id": 1, "_id": 0})
for lineup in lineups:
    persons = lineup["currentLineup"]
    for person in persons:
        personsIds.append(person["_id"])

print("Count of persons needed to be in DB: " + personsIds.__len__().__str__())

existingPersonsIds = []
existingPersons = db.persons.find({}, {"_id": 1})
for existingPerson in existingPersons:
    existingPersonsIds.append(existingPerson["_id"])

neededIds = list(set(personsIds) - set(existingPersonsIds))
urls_set = set()
for neededId in neededIds:
    urls_set.add(persons_url.format(neededId))

print("Count of persons needed to be added into DB: " + neededIds.__len__().__str__())


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


urls = list(urls_set)
list_a = list(chunks(urls, 1000))

for url_list in list_a:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(aaa(url_list))

# urls = []
# urls_set = set()
# for existingPersonsId in neededIds:
#     urls_set.add(read_more.format(existingPersonsId))
#
# urls = list(urls_set)
#
# loop2 = asyncio.get_event_loop()
# loop2.run_until_complete(bbb(urls))
