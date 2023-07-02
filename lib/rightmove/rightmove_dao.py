import logging
import os
from contextlib import contextmanager

from dotenv import load_dotenv
from pymongo import MongoClient

log = logging.getLogger('tracker')

load_dotenv()

# TODO: Store list of tracked collections in MongoDB alongside TRACKER_URLs
DB_CONNECTION = os.getenv('DB_CONNECTION')
DB_NAME = os.getenv('DB_NAME')
DB_COLLECTION = os.getenv('DB_COLLECTION')

log.info(f'Connecting using connection string: {DB_CONNECTION}, DB: {DB_NAME}, collection: {DB_COLLECTION}')


@contextmanager
def mongo_client():
    client = MongoClient(DB_CONNECTION)
    db = client[DB_NAME]
    collection = db[DB_COLLECTION]
    #collection = client.rightmove.listings
    #collection = client.rightmove.test_listings
    with client.start_session() as session:
        try:
            session.start_transaction()
            yield session, collection
            session.commit_transaction()
        except Exception as e:
            session.abort_transaction()
            raise

