import logging, datetime


def doc_show_filter(parsed_doc):
    working_status = parsed_doc.get('_c6_beit_hatfutsot_bh_base_template_working_status', [{}])[0].get("en")
    rights = parsed_doc.get('_c6_beit_hatfutsot_bh_base_template_rights', [{}])[0].get("en")
    display_status = parsed_doc.get('_c6_beit_hatfutsot_bh_base_template_display_status', [{}])[0].get("en")
    if bool(working_status == 'Completed' and rights == 'Full' and display_status not in ['Internal Use']):
        return True
    else:
        # warn_once("items are skipped due to show filter")
        return False


msg_cache = []

def warn_once(msg):
    if msg not in msg_cache:
        msg_cache.append(msg)
        logging.warning(msg)


def check_download_ttl(existing_ids, item_id):
    if item_id:
        item_row = existing_ids[int(item_id)]
        if item_row == True:
            # this signifies a row which was processed in current pipeline run and should be forcibly skipped
            return False
        else:
            last_downloaded, hours_to_next_download, last_synced = item_row
            now = datetime.datetime.now()
            next_download = last_downloaded + datetime.timedelta(hours=hours_to_next_download)
            seconds_to_next_download = (next_download - now).total_seconds()
            if seconds_to_next_download < 0:
                # should download the item
                return True
            else:
                # don't download
                return False
    else:
        return False


def update_download_ttl(existing_ids, item_id):
    last_synced, hours_to_next_download = None, None
    if item_id:
        item_id = int(item_id)
        hours_to_next_download = 5
        if item_id in existing_ids:
            item_row = existing_ids[item_id]
            if item_row == True:
                # this signifies a row which was processed in current pipeline run and should be forcibly skipped
                hours_to_next_download = None
            else:
                last_downloaded, hours_to_next_download, last_synced = item_row
                hours_to_next_download = hours_to_next_download * 2
                if hours_to_next_download > 24 * 14:
                    hours_to_next_download = 24 * 14
        if hours_to_next_download is not None and hours_to_next_download < 5:
            hours_to_next_download = 5
    return last_synced, hours_to_next_download
