from pymongo import MongoClient
from dateutil.parser import parse
from datetime import datetime
import sys

client = MongoClient("mongodb://vova:yfhrjvfy1@67.205.186.196:27017/admin?authMechanism=SCRAM-SHA-1")
db = client.on_metal_db

existing_albums = db.albums.find({}, {"_id": 1, "release date": 1})
existing_albums_ids = []
print("Total: " + existing_albums.count().__str__())
total_items = existing_albums.count()
for existing_album in existing_albums:
    dateString = existing_album["release date"]
    try:
        dt = parse(dateString)
        db.albums.update_one(
            {"_id": existing_album["_id"]},
            {"$set": {"release date": datetime(dt.year, dt.month, dt.day)}}
        )
    except:
        total_items
    total_items = total_items - 1
    sys.stdout.write('\r')
    sys.stdout.flush()
    sys.stdout.write(total_items.__str__() + " remain")
    sys.stdout.flush()

