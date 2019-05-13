import grequests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import urllib3

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


def done(response, *args, **kwargs):
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


def get_bio(response, *args, **kwargs):
    person_id = response.url.split("/")[6]
    persons_map[person_id] = BeautifulSoup(response.content, "html.parser").text


def get_redirect_urls(response, *args, **kwargs):
    redirect_urls.append(response.headers['location'])
    print("RedirectUrls size: " + redirect_urls.__len__().__str__())


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
# new = neededIds[:len(neededIds) - 495530]
# urls = []
urls_set = set()
for neededId in neededIds:
    urls_set.add(persons_url.format(neededId))
    # urls.append(persons_url.format(neededId))

print("Count of persons needed to be added into DB: " + neededIds.__len__().__str__())

urls = list(urls_set)

for u in urls:
    rq = grequests.get(u, hooks={'response': get_redirect_urls}, verify=False)
    grequests.map([rq])

for u in redirect_urls:
    rq = grequests.get(u, hooks={'response': done}, verify=False)
    grequests.map([rq])

# if len(urls) > 0:
#     rq = (grequests.get(u, hooks={'response': get_redirect_urls}, verify=False) for u in urls)
#     grequests.map(rq, size=10)
#     rq = (grequests.get(u, hooks={'response': done}, verify=False) for u in redirect_urls)
#     grequests.map(rq, size=10)

# urls = []
# urls_set = set()
# for neededId in neededIds:
#     urls_set.add(read_more.format(neededId))
#     # urls.append(read_more.format(neededId))
#
# urls = list(urls_set)
#
# if len(urls) > 0:
#     rq = (grequests.get(u, hooks={'response': get_bio}, verify=False) for u in urls)
#     grequests.map(rq, size=10)
#
# for one_person in persons_map.keys():
#     db.persons.update_one(
#         {"_id": one_person},
#         {"$set": {"details.bio": persons_map[one_person]}}
#     )
