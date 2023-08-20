import logging
import os
import sys

from dotenv import load_dotenv

from lib.rightmove.processor import UrlProcessor

load_dotenv()

logging.basicConfig(stream=sys.stdout, format='%(levelname)s:%(message)s')
log = logging.getLogger('tracker')
log.setLevel(logging.INFO)

# TODO: Store list of tracked URLS in mongodb
TRACKER_URL = os.getenv('TRACKER_URL')


def do_work():
    log.info("Running Rightmove Price Tracker")
    processor = UrlProcessor()
    processor.process_url(TRACKER_URL)

def query_data():
    # TODO: Something useful with the data
    processor = UrlProcessor()
    # processor.query_data()
    processor.query_stats()

#
# def email_tester():
#     rm = ImprovedRightmoveData(url)
#
#     from_web = rm.get_results
#     send_email(inserted_props=from_web, updated_props=[])

if __name__ == '__main__':
    # do_work()
    query_data()
    # email_tester()
