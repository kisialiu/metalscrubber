import re
import math
import grequests
import string
from pymongo import MongoClient
from bs4 import BeautifulSoup
import urllib3

letterUrl = "https://www.metal-archives.com/browse/ajax-letter/l/{}/json/1?sEcho={}&iColumns=4&sColumns=&iDisplayStart={}&iDisplayLength=500&mDataProp_0=0&mDataProp_1=1&mDataProp_2=2&mDataProp_3=3&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&bSortable_0=true&bSortable_1=true&bSortable_2=true&bSortable_3=false&_=1509737634817"
readMore = "https://www.metal-archives.com/band/read-more/id/{}"
discs = "https://www.metal-archives.com/band/discography/id/{}/tab/all"
searchLetters = list(string.ascii_uppercase) + ['NBR', '~']
# searchLetters = ['A']
bandsMap = {}

client = MongoClient("mongodb://vova:yfhrjvfy1@67.205.186.196:27017/admin?authMechanism=SCRAM-SHA-1")
db = client.on_metal_db
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
http = urllib3.PoolManager()

def getPagesNumber(letter):
    r = grequests.get(letterUrl.format(letter, 1, 0), verify=False)
    resp = grequests.map([r])[0]
    totalRecords = resp.json()['iTotalRecords']
    pages = totalRecords / 500
    return math.ceil(pages)

def isBandExist(id):
    cur = db.bands.find({"_id": id})
    return cur.count() != 0

def putBandsToMap(resp, *args, **kwargs):
    json = resp.json()["aaData"]
    for data in json:
        a = data[0]
        m = re.search("bands/(.*)'>", a)
        str = m.group(0)
        id = str.split('/')[2].split("'>")[0]
        if isBandExist(id) == False:
            bandName = re.search('https.*/\d{1,}', a).group(0)
            bandsMap[id] = {
                "_id": id,
                "bandName": bandName,
                "bio": "",
                "discography": []
            }

def getMembers(resultSet):
    list = []
    for tr in resultSet:
        info = tr.find_all('td')
        instruments = info[1].text.replace('\t','').replace('\n','')
        if instruments.find('(') > 0:
            isCurrent = instruments.find('present') > 0
        else:
            isCurrent = True

        member = {
            "_id": info[0].find('a')['href'].split('/')[5],
            "name": info[0].find('a').text,
            "current": isCurrent,
            "instruments": [x.strip() for x in instruments.split(',')]
        }
        list.append(member)
    return list

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

def done(response, *args, **kwargs):
    url = response.url
    id = url.split("/")[5]

    soup = BeautifulSoup(response.content, "html.parser")
    band_content = soup.find('div', attrs={'id': 'band_content'})

    lineUp = soup.find('table', attrs={'class': 'display lineupTable'})
    members = [] if lineUp is None else lineUp.find_all('tr', attrs={'class': 'lineupRow'})
    imgs = soup.find('div', attrs={'id': 'band_sidebar'}).find_all('img', src=True)
    imgsLen = len(imgs)
    band_stats = band_content.find('div', attrs={'id': 'band_stats'}).find_all('dl')

    json = {"_id": id,
            "details": {
                "country": band_stats[0].find_all('dd')[0].find('a').text,
                "location": band_stats[0].find_all('dd')[1].text,
                "status": band_stats[0].find_all('dd')[2].text,
                "formedIn": band_stats[0].find_all('dd')[3].text,
                "genre": band_stats[1].find_all('dd')[0].text,
                "lyricalThemes": band_stats[1].find_all('dd')[1].text,
                "currentLabel": band_stats[1].find_all('dd')[2].text,
                "yearsActive": band_stats[2].find_all('dd')[0].text.strip()
            },
            "bandName": band_content.find('h1', attrs={'class': 'band_name'}).find('a').text,
            "logo": imgs[0]['src'] if imgsLen > 0 else "",
            "photo": imgs[1]['src'] if imgsLen > 1 else "",
            "bio": bandsMap.get(id)["bio"],
            "discography": bandsMap.get(id)["discography"],
            "currentLineup": getMembers(members)
            }
    db.bands.insert_one(json)

def getBio(response, *args, **kwargs):
    # global bandsMap
    id = response.url.split("/")[6]
    bandsMap[id]["bio"] = BeautifulSoup(response.content, "html.parser").text

def getDiscography(response, *args, **kwargs):
    # global bandsMap
    id = response.url.split("/")[6]
    bandsMap[id]["discography"] = getDiscs(BeautifulSoup(response.content, "html.parser").find('tbody').find_all('tr'))

for letter in searchLetters:
    pages = getPagesNumber(letter)
    displayStart = 500
    for i in range(1, pages + 1, 1):
        letterReq = grequests\
            .get(letterUrl.format(letter, i, (displayStart * (i - 1)))
                 , hooks={'response': putBandsToMap}
                 , verify=False)
        grequests.map([letterReq])
        urls = list(x["bandName"] for x in list(bandsMap.values()))
        urls1 = list(readMore.format(x["_id"]) for x in list(bandsMap.values()))
        urls2 = list(discs.format(x["_id"]) for x in list(bandsMap.values()))
        rs1 = (grequests.get(u, hooks={'response': getBio}, verify=False) for u in urls1)
        grequests.map(rs1)
        rs2 = (grequests.get(u, hooks={'response': getDiscography}, verify=False) for u in urls2)
        grequests.map(rs2)
        rs = (grequests.get(u, hooks={'response': done}, verify=False) for u in urls)
        grequests.map(rs, size=10)
        bandsMap.clear()
