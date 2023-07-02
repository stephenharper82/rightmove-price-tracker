import logging as log

from lib.rightmove.processor import UrlProcessor
from dotenv import load_dotenv
import os
load_dotenv()
log.basicConfig(format='%(levelname)s:%(message)s', level=log.INFO)

# TODO: Store list of tracked URLS in mongodb
TRACKER_URL = os.getenv('TRACKER_URL')



# TODO Should store URLs being tracked in DB, and iterate over them. Means storing an ID related to the search against each listings collection too
URL = 'https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E68042&minBedrooms=4&maxPrice=650000&minPrice=375000&radius=15.0&propertyTypes=bungalow%2Cdetached%2Csemi-detached%2Cterraced&includeSSTC=true&mustHave=garden%2Cparking&dontShow=&furnishTypes=&keywords='


def do_work():
    log.info("Running Rightmove Price Tracker")
    processor = UrlProcessor()
    processor.process_url(TRACKER_URL)

def query_data():
    # TODO: Something useful with the data
    processor = UrlProcessor()
    processor.query_data()

#
# def email_tester():
#     rm = ImprovedRightmoveData(url)
#
#     from_web = rm.get_results
#     send_email(inserted_props=from_web, updated_props=[])

if __name__ == '__main__':
    do_work()
    #query_data()
    # email_tester()
