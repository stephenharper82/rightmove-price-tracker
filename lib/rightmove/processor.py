import datetime
import json
import logging as log
from typing import Any, Dict

import utils
from lib.rightmove.emailer import send_email
from lib.rightmove.local_scraper import ImprovedRightmoveData
from lib.rightmove.rightmove_dao import mongo_client


class UrlProcessor:

    # TODO: Something interesting with the data
    # def query_data(self):
    #     with mongo_client() as listings:
    #         db_entries = self.get_listings(listings)
    #         statuses ={p['status'] for p in db_entries}
    #
    #         not_sold = [p for p in db_entries if not p['status']]
    #
    #         log.info(f"Retrieved {len(db_entries)} filtered down to {len(not_sold)} not sold")
    #
    #         log.info(f"statuses: {statuses}")
    #
    #         log.info("HERE")
    #         #db_entries.sort()
    #
    #         def is_filter(prop:Dict[str,Any]):
    #             pass
    #
    #         def oldest_date(prop:Dict[str,Any]):
    #             if prop['audit_history']:

    def track_new_url(self, url: str, notification_email: str):
        data = {'url': url, 'notification_email': notification_email}
        # TODO: Create new collection in DB for this new URL.
        #  Move config into DB

    def process_urls(self):
        pass

    def get_listings(self, listings):
        cursor = listings.find({})
        # TODO Use generator instead.
        db_entries = list(cursor)
        return db_entries

    def process_url(self, url: str):

        rm = ImprovedRightmoveData(url)

        from_web = rm.get_results

        # TODO: Temp debugging to remove
        # log.info(f"DB ENTRIES { [x for x in d if  x['_id'] == '128729528']}")
        # TODO : session
        #  https://stackoverflow.com/questions/59264158/provide-contextvars-context-with-a-contextmanager
        with mongo_client() as listings:
            db_entries = self.get_listings(listings)

            db_changes = []
            full_changes = []
            to_be_inserted = []
            for webentry in from_web:
                dbentry = self._find_db_entry(db_entries, webentry)
                if dbentry:
                    merged = self._derive_changes(webentry, dbentry)
                    if merged:
                        full_doc, changes_doc = merged
                        db_changes.append(changes_doc)
                        full_changes.append(full_doc)
                else:
                    to_be_inserted.append(webentry)

            log.info(f"Updating {len(db_changes)} records")
            for rec in db_changes:
                listings.update_one({'_id': rec['_id']}, {"$set": rec})

            log.info(f"Inserting {len(to_be_inserted)} records")
            if to_be_inserted:
                listings.insert_many(to_be_inserted)

            file_contents = [f"Changes: \n\n {json.dumps(db_changes, indent=4)}",
                             f"\n\nNew: \n\n {json.dumps(to_be_inserted, indent=4)} \n\n\n FOR EMAIL\n\n {json.dumps(full_changes, indent=4)}"]
            utils.write_to_temp_file(file_contents, file_prefix="rightmove_raw_out", file_suffix=".txt")

            log.info("Finished updating DB... Now sending email...")
            send_email(inserted_props=to_be_inserted, updated_props=full_changes)
            # rm.get_results.to_csv("C:/Users/steph/temp/dat.csv", index=False)
            log.info("...Finished processing URL")

    def _find_db_entry(self, db_entries, webentry):
        return next(iter([x for x in db_entries if webentry['_id'] == x['_id']]), None)

    def _derive_changes(self, new_doc, db_doc):
        def value_or_na(mp, field):
            return mp[field] if field in mp and mp[field] is not None else "EMPTY"

        def is_added_or_reduced_on_change(field):
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

        updated_fields = {}
        latest_changes = {}
        audits = db_doc['audit_history'] if 'audit_history' in db_doc else []
        audit = {}
        for f in ["price", "price_type", "type", "address", "url", "agent_url", "added_or_reduced_on", "status"]:
            if f == 'added_or_reduced_on' and is_added_or_reduced_on_change(f):
                audit[f] = {'old': value_or_na(db_doc, f), 'new': value_or_na(new_doc, f)}
                updated_fields[f] = value_or_na(new_doc, f)
            elif f != 'added_or_reduced_on' and is_change(f):
                audit[f] = {'old': value_or_na(db_doc, f), 'new': value_or_na(new_doc, f)}
                updated_fields[f] = value_or_na(new_doc, f)

        if updated_fields:
            audit['on'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')
            audits.append(audit)
            updated_fields['audit_history'] = audits

            log.info(f"Changes for {db_doc['_id']} is {audit}")
            updated_fields['_id'] = db_doc['_id']
            log.info(f"changes obj: {updated_fields}")
            new_doc['audit_history'] = audits
            return new_doc, updated_fields
        else:
            return None
