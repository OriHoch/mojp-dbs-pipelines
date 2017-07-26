import datetime

def convert_doc(item_data):
    new_doc = {}
    title_en = item_data["Header"]["En"]
    main_image_url = item_data["main_image_url"]
    new_doc["source"] = "Bagnowka"
    new_doc["version"] = "1"
    new_doc["collection"] = "photoUnits"
    new_doc["source_doc"] = item_data
    new_doc["title"] = None
    new_doc["content_html"] = None
    new_doc["content_html_en"] = None
    new_doc["content_html_he"] = None
    new_doc["title_en"] = title_en
    new_doc["title_he"] = None
    new_doc["main_image_url"] = main_image_url
    new_doc["main_thumbnail_url"] = main_image_url
    new_doc["related_documents"] = None
    return new_doc