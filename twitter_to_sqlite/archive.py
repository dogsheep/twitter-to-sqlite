# Utilities for dealing with Twitter archives
import json

# Goal is to have a mapping of filename to a tuple with
# (callable, pk=) triples, where the callable
# takes the JSON from that file and returns a dictionary
# of tables that should be created {"tabe": [rows-to-upsert]}
transformers = {}


def register(filename, each, pk=None):
    def callback(data):
        return {filename: [item.get(each) for item in data]}

    transformers[filename] = (callback, pk)


def register_each(filename, pk=None):
    def inner(fn):
        def callback(data):
            return {filename: [fn(item) for item in data]}

        transformers[filename] = (callback, pk)

    return inner


def register_multi(filename):
    def inner(fn):
        transformers[filename] = (fn, None)

    return inner


def register_all(filename):
    def inner(fn):
        transformers[filename] = (fn, None)

    return inner


def extract_json(contents):
    # window.YTD.account_creation_ip.part0 = [ ... data ...]
    contents = contents.strip()
    if contents.startswith(b"window."):
        contents = contents.split(b" = ", 1)[1]
    return json.loads(contents)


register("account-creation-ip", each="accountCreationIp")
register("account-suspension", each="accountSuspension")
register("account-timezone", each="accountTimezone")
register("account", each="account")


@register_each("ad-engagements")
def ad_engagements(item):
    return item["ad"]["adsUserData"]["adEngagements"]


@register_each("ad-impressions")
def ad_impressions(item):
    return item["ad"]["adsUserData"]["adImpressions"]


@register_each("ad-mobile-conversions-attributed")
def ad_mobile_conversions_attributed(item):
    return item["ad"]["adsUserData"]["attributedMobileAppConversions"]


@register_each("ad-mobile-conversions-unattributed")
def ad_mobile_conversions_unattributed(item):
    return item["ad"]["adsUserData"]["unattributedMobileAppConversions"]


@register_each("ad-online-conversions-attributed")
def ad_online_conversions_attributed(item):
    return item["ad"]["adsUserData"]["attributedOnlineConversions"]


@register_each("ad-online-conversions-unattributed")
def ad_online_conversions_unattributed(item):
    return item["ad"]["adsUserData"]["unattributedOnlineConversions"]


@register_each("ageinfo")
def ageinfo(item):
    return item["ageMeta"]["ageInfo"]


register("block", each="blocking", pk="accountId")
register("connected-applications", each="connectedApplication", pk="id")
# register("contact", ...)
register("direct-message-group-headers", each="dmConversation", pk="conversationId")
register("direct-message-group", each="dmConversation", pk="conversationId")
register("direct-message-headers", each="dmConversation", pk="conversationId")
# pk for this one is NOT set, because there are dupes:
# TODO: These actually do warrant separate tables:
register("direct-message", each="dmConversation")

register("email-address-change", each="emailAddressChange")
register("follower", each="follower", pk="accountId")
register("following", each="following", pk="accountId")
register("ip-audit", each="ipAudit")
register("like", each="like", pk="tweetId")


@register_all("lists-created")
def lists_created(data):
    return {"lists-created": _list_from_common(data)}


@register_all("lists-member")
def lists_member(data):
    return {"lists-member": _list_from_common(data)}


@register_all("lists-subscribed")
def lists_subscribed(data):
    return {"lists-subscribed": _list_from_common(data)}


register("moment", each="moment", pk="momentId")
# register("mute", ...)


@register_all("ni-devices")
def lists_created(data):
    devices = []
    for block in data:
        block = block["niDeviceResponse"]
        category = list(block.keys())[0]
        details = list(block.values())[0]
        details["category"] = category
        devices.append(details)
    return {"ne-devices": devices}


# Skipped all the periscope- stuff for the moment


@register_multi("personalization")
def personalization(data):
    data = data[0]
    # As a multi, we get to return a dict of
    # table names => list of objects to insert
    to_create = {}
    demographics = data["p13nData"]["demographics"]
    to_create["personalization-demographics-languages"] = demographics["languages"]
    to_create["personalization-demographics-genderInfo"] = [demographics["genderInfo"]]
    to_create["personalization-interests"] = data["p13nData"]["interests"]["interests"]
    to_create["personalization-partnerInterests"] = data["p13nData"]["interests"][
        "partnerInterests"
    ]
    to_create["personalization-advertisers"] = [
        {"name": name}
        for name in data["p13nData"]["interests"]["audienceAndAdvertisers"][
            "advertisers"
        ]
    ]
    to_create["personalization-num-audiences"] = [
        {
            "numAudiences": data["p13nData"]["interests"]["audienceAndAdvertisers"][
                "numAudiences"
            ]
        }
    ]
    to_create["personalization-shows"] = [
        {"name": name} for name in data["p13nData"]["interests"]["shows"]
    ]
    to_create["personalization-locationHistory"] = [
        {"name": name} for name in data["p13nData"]["locationHistory"]
    ]
    to_create["personalization-inferredAgeInfo"] = [data["p13nData"]["inferredAgeInfo"]]
    return to_create


register("phone-number", each="device")
register("profile", each="profile")
# protected-history.js

register("saved-search", each="savedSearch", pk="savedSearchId")
# screen-name-change.js


@register_each("tweet", pk="id")
def tweet(item):
    for key in item:
        if key == "id" or key.endswith("_id"):
            item[key] = int(item[key])
    return item


register("verified", each="verified")


def _list_from_common(data):
    lists = []
    for block in data:
        for url in block["userListInfo"]["urls"]:
            bits = url.split("/")
            lists.append({"screen_name": bits[-3], "list_slug": bits[-1]})
    return lists


def import_from_file(db, filename, content):
    assert filename.endswith(".js"), "{} does not end with .js".format(filename)
    existing_tables = set(db.table_names())
    filename = filename[: -len(".js")]
    if filename not in transformers:
        print("{}: not yet implemented".format(filename))
        return
    transformer, pk = transformers.get(filename)
    data = extract_json(content)
    to_insert = transformer(data)
    for table, rows in to_insert.items():
        table_name = "archive_{}".format(table.replace("-", "_"))
        # Drop and re-create if it already exists
        if table_name in existing_tables:
            db[table_name].drop()
        if pk is not None:
            db[table_name].insert_all(rows, pk=pk, replace=True)
        else:
            db[table_name].insert_all(rows, hash_id="pk", replace=True)
