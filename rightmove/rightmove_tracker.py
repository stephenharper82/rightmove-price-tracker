import datetime
import pathlib
import re

import numpy as np

from local_scraper import ImprovedRightmoveData
from pymongo import MongoClient
import logging as log
from rightmove.emailer import send_email
from rightmove.utils import setup_logging

log.basicConfig(format='%(levelname)s:%(message)s', level=log.INFO)
# setup_logging()

url = 'https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E68042&minBedrooms=4&maxPrice=650000&minPrice=375000&radius=15.0&propertyTypes=bungalow%2Cdetached%2Csemi-detached%2Cterraced&includeSSTC=true&mustHave=garden%2Cparking&dontShow=&furnishTypes=&keywords='


def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = "mongodb+srv://user_002:uHQ87BEhFy7ZUmTj@cluster0.ihkzp.mongodb.net/?retryWrites=true&w=majority"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client['rightmove']


def web_to_mongo():
    db = get_database()
    log.info(f"listings: {db['listings']}")

    rm = ImprovedRightmoveData(url)

    from_web = rm.get_results

    cursor = db.listings.find({})
    db_entries = list(cursor)
    # log.info(f"DB ENTRIES { [x for x in d if  x['_id'] == '128729528']}")
    log.info(f"web ENTRIES {[x for x in from_web if x['_id'] == '128729528']}")

    to_be_updated = []
    full_updates = []
    to_be_inserted = []
    for webentry in from_web:
        dbentry = next(iter([x for x in db_entries if webentry['_id'] == x['_id']]), None)
        if dbentry:
            merged = merged_result(webentry, dbentry)
            if merged:
                full_doc, changes_doc = merged
                to_be_updated.append(changes_doc)
                full_updates.append(full_doc)
        else:
            to_be_inserted.append(webentry)

    log.info(f"Updating {len(to_be_updated)} records")
    for rec in to_be_updated:
        db.listings.update_one({'_id': rec['_id']}, {"$set": rec})

    if to_be_inserted:
        log.info(f"Inserting {len(to_be_inserted)} records")
        db.listings.insert_many(to_be_inserted)

    base_out = pathlib.Path('C:/Users/steph/rightmove')
    out_path = base_out / datetime.datetime.today().strftime('%Y%m%d')
    out_path.mkdir(parents=True, exist_ok=True)
    out_path = out_path / 'run-changes.txt'
    with open(out_path, 'w') as f:
        f.write(f"Changes: \n\n {str(to_be_updated)}")
        f.write(f"\n\nNew: \n\n {str(to_be_inserted)}")

    log.info("DONE")
    send_email(inserted_props=to_be_inserted, updated_props=full_updates)
    # rm.get_results.to_csv("C:/Users/steph/temp/dat.csv", index=False)


def merged_result(new_doc, db_doc):
    def value_or_na(mp, field):
        return mp[field] if field in mp and mp[field] is not None else "EMPTY"

    def is_status_change(field):
        db_v = value_or_na(db_doc, field).split(' ')[0]
        new_v = value_or_na(new_doc, field).split(' ')[0]
        if db_v != new_v:
            log.info(f"field {field} changed from {db_v} to {new_v}")
            return True
        return False

    def is_change(field):
        if value_or_na(db_doc, field) != value_or_na(new_doc, field):
            log.debug(f"field {field} changed from {value_or_na(db_doc, field)} to {value_or_na(new_doc, field)}")
            # return f"Field {field} changed from {value_or_na(db_doc, field)} to {value_or_na(new_doc, field)}"
            return True
        return False

    update = {}
    audits = db_doc['audit_history'] if 'audit_history' in db_doc else []
    audit = {}
    for f in ["price", "price_type", "type", "address", "url", "agent_url", "added_or_reduced_on", "status"]:
        if f == 'status' and is_status_change(f):
            audit[f] = {'old': value_or_na(db_doc, f), 'new': value_or_na(new_doc, f)}
            update[f] = value_or_na(new_doc, f)
        elif is_change(f):
            audit[f] = {'old': value_or_na(db_doc, f), 'new': value_or_na(new_doc, f)}
            update[f] = value_or_na(new_doc, f)

    if update:
        audit['on'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')
        audits.append(audit)
        update['audit_history'] = audits

        log.info(f"Changes for {db_doc['_id']} is {audit}")
        update['_id'] = db_doc['_id']
        log.info(f"changes obj: {update}")
        db_doc['audit_history'] = audits
        return db_doc, update
    else:
        return None

def email_tester():
    rm = ImprovedRightmoveData(url)

    from_web = rm.get_results
    send_email(inserted_props=from_web, updated_props=[])

if __name__ == '__main__':
    web_to_mongo()
    #email_tester()
