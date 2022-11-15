import datetime
import re

import numpy as np

from local_scraper import ImprovedRightmoveData
from pymongo import MongoClient
import logging as log
from rightmove.utils import setup_logging

log.basicConfig(format='%(levelname)s:%(message)s', level=log.INFO)
# setup_logging()

url = 'https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E68042&minBedrooms=4&maxPrice=650000&minPrice=375000&radius=15.0&propertyTypes=bungalow%2Cdetached%2Csemi-detached%2Cterraced&includeSSTC=true&mustHave=garden%2Cparking&dontShow=&furnishTypes=&keywords='


def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = "mongodb+srv://user_002:8yd7XXHf2BQ7IIus@cluster0.ihkzp.mongodb.net/?retryWrites=true&w=majority"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client['rightmove']


def web_to_mongo():
    db = get_database()
    log.info(f"listings: {db['listings']}")

    rm = ImprovedRightmoveData(url)

    df = rm.get_results
    df['price_type'] = df['price_type'].apply(lambda x: str(x).strip() if x else x)
    df['_id'] = df['url'].apply(lambda x: re.search("http://www\.rightmove\.co\.uk/properties/(.+?)#", x).group(1))

    df = df.drop_duplicates(subset='_id', keep='last')
    # df.groupby('_id')
    # dropid = df.loc[
    #     df.duplicated(subset=['_id', 'timestamp']),
    #     'userid'
    # ].unique()

    # re.search("http://www\.rightmove\.co\.uk/properties/(.+?)#", "http://www.rightmove.co.uk/properties/123814832#/?channel=RES_BUY").group(1)
    df = df.replace({np.nan: None})
    from_web = df.to_dict('records')
    # db.listings.insert_many()

    cursor = db.listings.find({})
    db_entries  = list(cursor)
    #log.info(f"DB ENTRIES { [x for x in d if  x['_id'] == '128729528']}")
    log.info(f"web ENTRIES { [x for x in from_web if  x['_id'] == '128729528']}")

    from_web = df.to_dict('records')

    to_be_updated = []
    to_be_inserted = []
    for webentry in from_web:
        dbentry = next(iter([x for x in db_entries if webentry['_id'] == x['_id']]), None)
        if dbentry:
            merged = merged_result(webentry, dbentry)
            if merged:
                to_be_updated.append(merged)
        else:
            to_be_inserted.append(webentry)

    log.info(f"Updating {len(to_be_updated)} records")
    for rec in to_be_updated:
        db.listings.update_one({'_id': rec['_id']}, {"$set": rec})

    if to_be_inserted:
        log.info(f"Inserting {len(to_be_inserted)} records")
        db.listings.insert_many(to_be_inserted)

    log.info("DONE")
    # rm.get_results.to_csv("C:/Users/steph/temp/dat.csv", index=False)


def merged_result(new_doc, db_doc):
    def value_or_na(mp, field):
        return mp[field] if field in mp and mp[field] is not None else "EMPTY"

    def is_change(field):
        if new_doc[field] != db_doc[field]:
            log.debug(f"field {field} changed from {value_or_na(db_doc, field)} to {value_or_na(new_doc, field)}")
            return f"Field {field} changed from {value_or_na(db_doc, field)} to {value_or_na(new_doc, field)}"
    update = {}
    changed_fields = []
    for f in ["price", "price_type", "type", "address", "url", "agent_url", "added_or_reduced_on", "status"]:
        changed_msg = is_change(f)

        if changed_msg:

            update[f] = value_or_na(new_doc, f)
            changed_fields.append(changed_msg)

    if changed_fields:
        changed_fields.append(f"Changes on {datetime.datetime.now()}")

        changes_msg = ", ".join(changed_fields)

        msg_ = db_doc['changes'] + [changes_msg] if 'changes' in db_doc and db_doc['changes'] is not None else [
            changes_msg]
        log.info(f"Change message for {db_doc['_id']} is {msg_}")
        update['changes'] = msg_
        update['_id'] = db_doc['_id']
        log.info(f"changes obj: {update}")
        return update
    else:
        return None

if __name__ == '__main__':
    web_to_mongo()
