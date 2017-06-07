import iso639
import json

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
    dbs_row[attribute_prefix] = json.dumps(dbs_row[attribute_prefix])
