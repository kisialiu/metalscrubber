import re
import math
import grequests
from pymongo import MongoClient
from bs4 import BeautifulSoup
import urllib3
import sys

url = "https://www.metal-archives.com/archives/ajax-band-list/selection/{}/" \
      "by/created//json/1?sEcho=1&iColumns=6&sColumns=&iDisplayStart={}" \
      "&iDisplayLength=200" \
      "&mDataProp_0=0&mDataProp_1=1&mDataProp_2=2&mDataProp_3=3&mDataProp_4=4&mDataProp_5=5&iSortCol_0=4&sSortDir_0=desc&iSortingCols=1" \
      "&bSortable_0=true&bSortable_1=true&bSortable_2=true&bSortable_3=true&bSortable_4=true&bSortable_5=true&_=1519004579445"
readMore = "https://www.metal-archives.com/band/read-more/id/{}"
discs = "https://www.metal-archives.com/band/discography/id/{}/tab/all"
bandsMap = {}
year_month = ["2017-09", "2017-10", "2017-11", "2017-12", "2018-01", "2018-02", "2018-03"]

client = MongoClient("mongodb://vova:yfhrjvfy1@67.205.186.196:27017/admin?authMechanism=SCRAM-SHA-1")
db = client.on_metal_db
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
http = urllib3.PoolManager()

total_items = 0


def get_pages_number(total_records):
    pages_count = total_records / 200
    return math.ceil(pages_count)


def get_total_records(year_mon):
    r = grequests.get(url.format(year_mon, 0), verify=False)
    resp = grequests.map([r])[0]
    total_records = resp.json()['iTotalRecords']
    return total_records


def put_bands_to_map(resp, *args, **kwargs):
    json = resp.json()["aaData"]
    for data in json:
        a = data[1]
        m = re.search("bands/(.*)\"", a)
        band_id = m.group(0).split('/')[2].split("\"")[0]
        band_name = re.search('https.*/\d{1,}', a).group(0)
        bandsMap[band_id] = {
            "_id": band_id,
            "bandName": band_name,
            "bio": "",
            "discography": []
        }


def get_members(result_set):
    members_list = []
    for tr in result_set:
        info = tr.find_all('td')
        instruments = info[1].text.replace('\t', '').replace('\n', '')
        if instruments.find('(') > 0:
            is_current = instruments.find('present') > 0
        else:
            is_current = True

        member = {
            "_id": info[0].find('a')['href'].split('/')[5],
            "name": info[0].find('a').text,
            "current": is_current,
            "instruments": [x.strip() for x in instruments.split(',')]
        }
        members_list.append(member)
    return members_list


def get_discs(result_set):
    discs_list = []
    for tr in result_set:
        info = tr.find_all('td')
        disc = {
            "_id": info[0].find('a')['href'].split("/")[6],
            "name": info[0].find('a').text,
            "type": info[1].text,
            "year": info[2].text
        }
        discs_list.append(disc)
    return discs_list


def done(response, *args, **kwargs):
    band_url = response.url
    band_id = band_url.split("/")[5]

    soup = BeautifulSoup(response.content, "html.parser")
    band_content = soup.find('div', attrs={'id': 'band_content'})

    line_up = soup.find('table', attrs={'class': 'display lineupTable'})
    members = [] if line_up is None else line_up.find_all('tr', attrs={'class': 'lineupRow'})
    imgs = soup.find('div', attrs={'id': 'band_sidebar'}).find_all('img', src=True)
    imgs_len = len(imgs)
    band_stats = band_content.find('div', attrs={'id': 'band_stats'}).find_all('dl')

    json = {"_id": band_id,
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
            "logo": imgs[0]['src'] if imgs_len > 0 else "",
            "photo": imgs[1]['src'] if imgs_len > 1 else "",
            "bio": bandsMap.get(band_id)["bio"],
            "discography": bandsMap.get(band_id)["discography"],
            "currentLineup": get_members(members)
            }
    db.bands.save(json)
    global total_items
    total_items = total_items - 1
    restart_line()
    print_line(total_items.__str__() + " remain")
    # print("Added band: " + band_id)


def get_bio(response, *args, **kwargs):
    # global bandsMap
    band_id = response.url.split("/")[6]
    bandsMap[band_id]["bio"] = BeautifulSoup(response.content, "html.parser").text
    # print("Bio was gotten for band: " + band_id)


def get_discography(response, *args, **kwargs):
    # global bandsMap
    band_id = response.url.split("/")[6]
    bandsMap[band_id]["discography"] = get_discs(BeautifulSoup(response.content, "html.parser").find('tbody').find_all('tr'))
    # print("Discs were gotten for band: " + band_id)


def restart_line():
    sys.stdout.write('\r')
    sys.stdout.flush()


def print_line(line):
    sys.stdout.write(line)
    sys.stdout.flush()


for year_m in year_month:
    print("Working on year-month: " + year_m)
    total_items = get_total_records(year_m)
    pages = get_pages_number(total_items)
    # print("Pages number = " + pages.__str__())
    print("Items count = " + total_items.__str__())
    displayStart = 200
    for i in range(1, pages + 1, 1):
        # print("Page " + i.__str__() + " from " + pages.__str__())
        letterReq = grequests.get(url.format(year_m, displayStart * (i - 1))
                     , hooks={'response': put_bands_to_map}
                     , verify=False)
        grequests.map([letterReq])
        urls = list(x["bandName"] for x in list(bandsMap.values()))
        urls1 = list(readMore.format(x["_id"]) for x in list(bandsMap.values()))
        urls2 = list(discs.format(x["_id"]) for x in list(bandsMap.values()))
        rs1 = (grequests.get(u, hooks={'response': get_bio}, verify=False) for u in urls1)
        rs2 = (grequests.get(u, hooks={'response': get_discography}, verify=False) for u in urls2)
        rs = (grequests.get(u, hooks={'response': done}, verify=False) for u in urls)
        grequests.map(rs1, size=10)
        grequests.map(rs2, size=10)
        grequests.map(rs, size=10)
        bandsMap.clear()
        # print("Done")
