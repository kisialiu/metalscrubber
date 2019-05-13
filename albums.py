import grequests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import urllib3
from dateutil.parser import parse
from datetime import datetime

albums_url = "https://www.metal-archives.com/albums/_/_/{}"
client = MongoClient("mongodb://vova:yfhrjvfy1@67.205.186.196:27017/admin?authMechanism=SCRAM-SHA-1")
db = client.on_metal_db
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
http = urllib3.PoolManager()


def get_tracks(result_set):
    tracks = []
    for tr in result_set:
        td = tr.find_all('td')
        track = {
            "title": td[1].text.strip(),
            "length": td[2].text
        }
        tracks.append(track)
    return tracks


def get_persons(result_set):
    persons = []
    for tr in result_set:
        td = tr.find_all('td')
        person = {
            "name": td[0].text.strip(),
            "_id": td[0].find('a')['href'].split('/')[5],
            "role": td[1].text.strip()
        }
        persons.append(person)
    return persons


def done(response, *args, **kwargs):
    album_id = response.url.split("/")[6]
    soup = BeautifulSoup(response.content, "html.parser")
    album_info = soup.find('div', attrs={'id': 'album_info'})
    left = album_info.find_all('dl')[0].find_all('dd')
    right = album_info.find_all('dl')[1].find_all('dd')
    tracks = soup.find('div', attrs={'id': 'album_tabs_tracklist'}).find('tbody').find_all('tr', attrs={
        'class': ['even', 'odd']})
    personnel = soup.find_all('tr', attrs={'class': 'lineupRow'})
    album_img = soup.find('div', attrs={'class': 'album_img'})
    dt = parse(left[1].text)

    album = {
        "title": album_info.find('h1').text,
        "_id": album_id,
        "band": {
            "_id": album_info.find('h2').find('a')['href'].split('/')[5],
            "name": album_info.find('h2').text.strip()
        },
        "album_cover": album_img.find('img')['src'] if album_img is not None else "",
        "type": left[0].text,
        "release date": datetime(dt.year, dt.month, dt.day),
        "catalog id": left[2].text,
        "label": right[0].text,
        "format": right[1].text,
        "reviews": right[2].text.strip(),
        "songs": get_tracks(tracks),
        "personnel": get_persons(personnel)
    }
    db.albums.insert_one(album)
    print("Added album: " + album_id)


albums_ids = []
albums = db.bands.find({}, {"discography._id": 1, "_id": 0})
for albumId in albums:
    discs = albumId["discography"]
    for disc in discs:
        albums_ids.append(disc["_id"])

print("Count of albums needed to be in DB: " + albums_ids.__len__().__str__())

existing_albums = db.albums.find({}, {"_id": 1})
existing_albums_ids = []
for existing_album in existing_albums:
    existing_albums_ids.append(existing_album["_id"])

needed_ids = list(set(albums_ids) - set(existing_albums_ids))
urls = []
for needed_id in needed_ids:
    urls.append(albums_url.format(needed_id))

print("Count of albums needed to be added into DB: " + needed_ids.__len__().__str__())

if len(urls) > 0:
    rq = (grequests.get(u, hooks={'response': done}, verify=False) for u in urls)
    grequests.map(rq, size=10)
