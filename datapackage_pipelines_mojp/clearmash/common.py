import logging


def doc_show_filter(parsed_doc):
    working_status = parsed_doc.get('_c6_beit_hatfutsot_bh_base_template_working_status', [{}])[0].get("en")
    rights = parsed_doc.get('_c6_beit_hatfutsot_bh_base_template_rights', [{}])[0].get("en")
    display_status = parsed_doc.get('_c6_beit_hatfutsot_bh_base_template_display_status', [{}])[0].get("en")
    if bool(working_status == 'Completed' and rights == 'Full' and display_status not in ['Internal Use']):
        return True
    else:
        warn_once("items are skipped due to show filter")
        return False


msg_cache = []

def warn_once(msg):
    if msg not in msg_cache:
        msg_cache.append(msg)
        logging.warning(msg)
