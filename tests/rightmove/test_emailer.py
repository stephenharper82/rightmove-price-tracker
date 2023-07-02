import json
import pathlib

import emailer

test_input = pathlib.Path(__file__).parent.parent / 'resources' / 'sample_scraper_out.json'


def test_emailer():
    with open(test_input) as ti:
        data = json.load(ti)

        print(emailer.translate(inserted_props=[], updated_props=data))
