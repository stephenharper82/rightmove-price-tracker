import json

import requests
from bs4 import BeautifulSoup
from lxml import html


class ImprovedRightmoveData:

    def __init__(self, url):
        self._status_code, self._first_page = self._request(url)
        self._url = url
        self._results = self._get_results()

    @staticmethod
    def _request(url: str):
        r = requests.get(url)
        return r.status_code, r.content

    @property
    def url(self):
        return self._url

    @property
    def get_results(self):
        """Return results,but detect dupes and return a single value only"""
        return [i for n, i in enumerate(self._results) if i not in self._results[n + 1:]]

    def _get_results(self):
        """Build a Pandas DataFrame with all results returned by the search."""
        results = self._get_page(self._first_page)

        # Iterate through all pages scraping results:
        for p in range(1, self.page_count + 1, 1):

            # Create the URL of the specific results page:
            p_url = f"{str(self.url)}&index={p * 24}"

            # Make the request:
            status_code, content = self._request(p_url)

            # Requests to scrape lots of pages eventually get status 400, so:
            if status_code != 200:
                break

            # Create a temporary DataFrame of page results:
            props = self._get_page(content)

            # Concatenate the temporary DataFrame with the full DataFrame:

            results.extend(props)

        return results

    def _get_page(self, request_content: str):

        """Method to scrape data from a single page of search results. Used
        iteratively by the `get_results` method to scrape data from every page
        returned by the search.

        Note: response contains JSON containing all the data we need, so use this as opposed to scraping"""

        soup = BeautifulSoup(request_content, features="lxml")

        # Process the html:

        all_scripts = soup.find_all('script')

        json_model_script = next(
            script.getText() for script in all_scripts if script.getText().startswith('window.jsonModel ='))
        json_model_str = json_model_script.split('window.jsonModel = ', 1)[1]
        properties = json.loads(json_model_str)

        result_properties = []
        for property in properties['properties']:
            # Convert response JSON to format used by this app
            result_property = {}
            # Use _id for mongodb key
            result_property['_id'] = property['id']
            result_property['price'] = property['price']['amount']
            result_property['price_type'] = property['price']['displayPrices'][0]['displayPriceQualifier']
            result_property['type'] = property['propertyTypeFullDescription']
            result_property['address'] = property['displayAddress']
            result_property['url'] = property['propertyUrl']
            result_property['agent_url'] = property['contactUrl']
            result_property['added_or_reduced_on'] = property['addedOrReduced']
            result_property['bedrooms'] = property['bedrooms']
            result_property['bathrooms'] = property['bathrooms']
            result_property['image'] = property['propertyImages']['mainMapImageSrc']
            result_property['propertySubType'] = property['propertySubType']
            result_property['status'] = property['displayStatus']
            result_properties.append(result_property)

        return result_properties

    @property
    def page_count(self):
        """Returns the number of result pages returned by the search URL. There
        are 24 results per page. Note that the website limits results to a
        maximum of 42 accessible pages."""
        page_count = self.results_count_display // 24
        if self.results_count_display % 24 > 0:
            page_count += 1
        # Rightmove will return a maximum of 42 results pages, hence:
        if page_count > 42:
            page_count = 42
        return page_count

    @property
    def results_count_display(self):
        """Returns an integer of the total number of listings as displayed on
        the first page of results. Note that not all listings are available to
        scrape because rightmove limits the number of accessible pages."""
        tree = html.fromstring(self._first_page)
        xpath = """//span[@class="searchHeader-resultCount"]/text()"""
        return int(tree.xpath(xpath)[0].replace(",", ""))
