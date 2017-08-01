import iso639, importlib
from elasticsearch import Elasticsearch

KNOWN_LANGS = iso639.languages.part1.keys()


def populate_iso_639_language_field(dbs_row, attribute_prefix, source_lang_dict):
    # we use this approach for dealing with multiple languages
    # https://www.elastic.co/guide/en/elasticsearch/guide/current/one-lang-fields.html
    dbs_row.update({attribute_prefix: {},
                    "{}_he".format(attribute_prefix): None,
                    "{}_en".format(attribute_prefix): None})
    if source_lang_dict:
        for lang, title in source_lang_dict.items():
            if lang not in KNOWN_LANGS:
                raise Exception("language id not according to iso639 standard: {}".format(lang))
            elif lang in ["he", "en"]:
                dbs_row["{}_{}".format(attribute_prefix, lang)] = title
            else:
                dbs_row[attribute_prefix][lang] = title
    dbs_row[attribute_prefix] = dbs_row[attribute_prefix]


def get_elasticsearch(settings):
    return Elasticsearch(settings.MOJP_ELASTICSEARCH_DB), settings.MOJP_ELASTICSEARCH_INDEX


def parse_import_func_parameter(value, *args):
    if value and value.startswith("(") and value.endswith(")"):
        cmdparts = value[1:-1].split(":")
        cmdmodule = cmdparts[0]
        cmdfunc = cmdparts[1]
        cmdargs = cmdparts[2] if len(cmdparts) > 2 else None
        func = importlib.import_module(cmdmodule)
        for part in cmdfunc.split("."):
            func = getattr(func, part)
        if cmdargs == "args":
            value = func(*args)
        else:
            value = func()
    return value
