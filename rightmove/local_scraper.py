from rightmove_webscraper import RightmoveData
import json
from bs4 import BeautifulSoup
import datetime
from lxml import html
import numpy as np
import pandas as pd
import requests


class ImprovedRightmoveData(RightmoveData):


    def _get_page(self, request_content: str, get_floorplans: bool = False):

        """Method to scrape data from a single page of search results. Used
        iteratively by the `get_results` method to scrape data from every page
        returned by the search."""
        # Process the html:
        tree = html.fromstring(request_content)

        # Set xpath for price:
        if "rent" in self.rent_or_sale:
            xp_prices = """//span[@class="propertyCard-priceValue"]/text()"""
        elif "sale" in self.rent_or_sale:
            xp_prices = """//div[@class="propertyCard-priceValue"]/text()"""
        else:
            raise ValueError("Invalid URL format.")

        # Set xpaths for listing title, property address, URL, and agent URL:
        xp_titles = """//div[@class="propertyCard-details"]\
        //a[@class="propertyCard-link"]\
        //h2[@class="propertyCard-title"]/text()"""
        xp_addresses = """//address[@class="propertyCard-address"]//span/text()"""
        xp_weblinks = """//div[@class="propertyCard-details"]//a[@class="propertyCard-link"]/@href"""
        xp_agent_urls = """//div[@class="propertyCard-contactsItem"]\
        //div[@class="propertyCard-branchLogo"]\
        //a[@class="propertyCard-branchLogo-link"]/@href"""

        xp_added_or_reduced = """//span[@class="propertyCard-branchSummary-addedOrReduced"]/text()"""
        xp_status = """//div[@class="propertyCard-tag"]/span[@class="propertyCard-tagTitle propertyCard-tagTitle--display-status"]/text()"""
        xp_price_type = """//div[@class="propertyCard-priceQualifier"]//span[contains(concat(' ', normalize-space(@data-bind), ' '),"onGuidePriceClick")]/text()"""

        # Create data lists from xpaths:
        price_pcm = tree.xpath(xp_prices)
        price_types = tree.xpath(xp_price_type)
        titles = tree.xpath(xp_titles)
        addresses = tree.xpath(xp_addresses)
        added_or_reduceds = tree.xpath(xp_added_or_reduced)
        statuses = tree.xpath(xp_status)

        base = "http://www.rightmove.co.uk"
        weblinks = [f"{base}{tree.xpath(xp_weblinks)[w]}" for w in range(len(tree.xpath(xp_weblinks)))]
        agent_urls = [f"{base}{tree.xpath(xp_agent_urls)[a]}" for a in range(len(tree.xpath(xp_agent_urls)))]

        # Optionally get floorplan links from property urls (longer runtime):
        floorplan_urls = list() if get_floorplans else np.nan
        if get_floorplans:
            for weblink in weblinks:
                status_code, content = self._request(weblink)
                if status_code != 200:
                    continue
                tree = html.fromstring(content)
                xp_floorplan_url = """//*[@id="floorplanTabs"]/div[2]/div[2]/img/@src"""
                floorplan_url = tree.xpath(xp_floorplan_url)
                if floorplan_url:
                    floorplan_urls.append(floorplan_url[0])
                else:
                    floorplan_urls.append(np.nan)

        # Store the data in a Pandas DataFrame:
        data = [price_pcm, price_types, titles, addresses, weblinks, agent_urls, added_or_reduceds, statuses]
        data = data + [floorplan_urls] if get_floorplans else data
        temp_df = pd.DataFrame(data)
        temp_df = temp_df.transpose()
        columns = ["price", "price_type", "type", "address", "url", "agent_url", "added_or_reduced_on", "status"]
        columns = columns + ["floorplan_url"] if get_floorplans else columns
        temp_df.columns = columns

        # Drop empty rows which come from placeholders in the html:
        temp_df = temp_df[temp_df["address"].notnull()]

        return temp_df

    def _get_page_old(self, request_content: str, get_floorplans: bool = False):
        """Method to scrape data from a single page of search results. Used
        iteratively by the `get_results` method to scrape data from every page
        returned by the search."""
        # Process the html:
        tree = html.fromstring(request_content)

        # Set xpath for price:
        if "rent" in self.rent_or_sale:
            xp_prices = """//span[@class="propertyCard-priceValue"]/text()"""
        elif "sale" in self.rent_or_sale:
            xp_prices = """//div[@class="propertyCard-priceValue"]/text()"""
        else:
            raise ValueError("Invalid URL format.")

        # Set xpaths for listing title, property address, URL, and agent URL:
        xp_titles = """//div[@class="propertyCard-details"]\
        //a[@class="propertyCard-link"]\
        //h2[@class="propertyCard-title"]/text()"""
        xp_addresses = """//address[@class="propertyCard-address"]//span/text()"""
        xp_weblinks = """//div[@class="propertyCard-details"]//a[@class="propertyCard-link"]/@href"""
        xp_agent_urls = """//div[@class="propertyCard-contactsItem"]\
        //div[@class="propertyCard-branchLogo"]\
        //a[@class="propertyCard-branchLogo-link"]/@href"""

        xp_added_or_reduced = """//span[@class="propertyCard-branchSummary-addedOrReduced"]/text()"""
        xp_status = """//div[@class="propertyCard-tag"]/span[@class="propertyCard-tagTitle propertyCard-tagTitle--display-status"]/text()"""
        xp_price_type = """//div[@class="propertyCard-priceQualifier"]//span[contains(concat(' ', normalize-space(@data-bind), ' '),"onGuidePriceClick")]/text()"""

        # Create data lists from xpaths:
        price_pcm = tree.xpath(xp_prices)
        price_types = tree.xpath(xp_price_type)
        titles = tree.xpath(xp_titles)
        addresses = tree.xpath(xp_addresses)
        added_or_reduceds = tree.xpath(xp_added_or_reduced)
        statuses = tree.xpath(xp_status)

        base = "http://www.rightmove.co.uk"
        weblinks = [f"{base}{tree.xpath(xp_weblinks)[w]}" for w in range(len(tree.xpath(xp_weblinks)))]
        agent_urls = [f"{base}{tree.xpath(xp_agent_urls)[a]}" for a in range(len(tree.xpath(xp_agent_urls)))]

        # Optionally get floorplan links from property urls (longer runtime):
        floorplan_urls = list() if get_floorplans else np.nan
        if get_floorplans:
            for weblink in weblinks:
                status_code, content = self._request(weblink)
                if status_code != 200:
                    continue
                tree = html.fromstring(content)
                xp_floorplan_url = """//*[@id="floorplanTabs"]/div[2]/div[2]/img/@src"""
                floorplan_url = tree.xpath(xp_floorplan_url)
                if floorplan_url:
                    floorplan_urls.append(floorplan_url[0])
                else:
                    floorplan_urls.append(np.nan)

        # Store the data in a Pandas DataFrame:
        data = [price_pcm, price_types, titles, addresses, weblinks, agent_urls, added_or_reduceds, statuses]
        data = data + [floorplan_urls] if get_floorplans else data
        temp_df = pd.DataFrame(data)
        temp_df = temp_df.transpose()
        columns = ["price", "price_type", "type", "address", "url", "agent_url", "added_or_reduced_on", "status"]
        columns = columns + ["floorplan_url"] if get_floorplans else columns
        temp_df.columns = columns

        # Drop empty rows which come from placeholders in the html:
        temp_df = temp_df[temp_df["address"].notnull()]

        return temp_df
