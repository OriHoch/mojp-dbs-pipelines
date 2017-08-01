import logging, os

def get_override_item_ids_where():
    override_ids = os.environ.get("OVERRIDE_CLEARMASH_ITEM_IDS")
    if override_ids:
        override_ids = override_ids.split(",")
        where = "item_id in ({})".format(",".join(["'{}'".format(id) for id in override_ids]))
        logging.info("where {}".format(where))
        return where
    else:
        return None


def download_new_entities_where():
    return get_override_item_ids_where()


def new_entities_sync_where():
    where = "hours_to_next_sync = 0"
    item_ids_where = get_override_item_ids_where()
    if item_ids_where:
        where = "{} and {}".format(where, item_ids_where)
    return where


def hours_to_next_sync(val):
    if not val or val == "0":
        return 5
    elif val < 24*7:
        return val * 2
    else:
        return 24*7
