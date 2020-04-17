import json
import pathlib

import pytest
import sqlite_utils
from twitter_to_sqlite import utils


@pytest.fixture
def tweets():
    return json.load(open(pathlib.Path(__file__).parent / "tweets.json"))


@pytest.fixture
def db(tweets):
    db = sqlite_utils.Database(memory=True)
    utils.save_tweets(db, tweets)
    return db


def test_tables(db):
    assert {
        "sources",
        "users_fts_idx",
        "users_fts_data",
        "tweets_fts",
        "tweets_fts_idx",
        "tweets",
        "users",
        "places",
        "following",
        "tweets_fts_data",
        "users_fts_config",
        "users_fts",
        "tweets_fts_config",
        "tweets_fts_docsize",
        "users_fts_docsize",
        "media",
        "media_tweets",
        "since_id_types",
        "since_ids",
        "count_history_types",
        "count_history",
    } == set(db.table_names())
    # And check for indexes
    following_indexes = {tuple(i.columns) for i in db["following"].indexes}
    assert {
        ("followed_id", "follower_id"),
        ("followed_id",),
        ("follower_id",),
    } == following_indexes


def test_users(db):
    user_rows = list(db["users"].rows)
    assert [
        {
            "id": 12497,
            "name": "Simon Willison",
            "screen_name": "simonw",
            "location": "San Francisco, CA",
            "description": "Creator of Datasette, co-creator Django. Fellow at @JSKstanford. Usually hanging out with @natbat and @cleopaws. He/Him",
            "url": "https://simonwillison.net/",
            "protected": 0,
            "followers_count": 17754,
            "friends_count": 3460,
            "listed_count": 1230,
            "created_at": "2006-11-15T13:18:50+00:00",
            "favourites_count": 21506,
            "utc_offset": None,
            "time_zone": None,
            "geo_enabled": 1,
            "verified": 1,
            "statuses_count": 17780,
            "lang": None,
            "contributors_enabled": 0,
            "is_translator": 0,
            "is_translation_enabled": 0,
            "profile_background_color": "000000",
            "profile_background_image_url": "http://abs.twimg.com/images/themes/theme1/bg.png",
            "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme1/bg.png",
            "profile_background_tile": 0,
            "profile_image_url": "http://pbs.twimg.com/profile_images/378800000261649705/be9cc55e64014e6d7663c50d7cb9fc75_normal.jpeg",
            "profile_image_url_https": "https://pbs.twimg.com/profile_images/378800000261649705/be9cc55e64014e6d7663c50d7cb9fc75_normal.jpeg",
            "profile_banner_url": "https://pbs.twimg.com/profile_banners/12497/1347977147",
            "profile_link_color": "0000FF",
            "profile_sidebar_border_color": "FFFFFF",
            "profile_sidebar_fill_color": "FFFFFF",
            "profile_text_color": "000000",
            "profile_use_background_image": 1,
            "has_extended_profile": 1,
            "default_profile": 0,
            "default_profile_image": 0,
            "following": 0,
            "follow_request_sent": 0,
            "notifications": 0,
            "translator_type": "regular",
        },
        {
            "id": 14148390,
            "name": "Brian Whitman",
            "screen_name": "bwhitman",
            "location": "Fort Greene NYC",
            "description": "finding the good @ourcanopy with the best people. was CTO/cofounder of Echo Nest, then research @ Spotify. always music",
            "url": "https://notes.variogr.am/about/",
            "protected": 0,
            "followers_count": 4300,
            "friends_count": 639,
            "listed_count": 235,
            "created_at": "2008-03-14T18:19:20+00:00",
            "favourites_count": 8966,
            "utc_offset": None,
            "time_zone": None,
            "geo_enabled": 1,
            "verified": 0,
            "statuses_count": 2192,
            "lang": None,
            "contributors_enabled": 0,
            "is_translator": 0,
            "is_translation_enabled": 0,
            "profile_background_color": "FFFFFF",
            "profile_background_image_url": "http://abs.twimg.com/images/themes/theme13/bg.gif",
            "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme13/bg.gif",
            "profile_background_tile": 0,
            "profile_image_url": "http://pbs.twimg.com/profile_images/742302060/avatars-000000620200-z21ozh-crop_normal.jpeg",
            "profile_image_url_https": "https://pbs.twimg.com/profile_images/742302060/avatars-000000620200-z21ozh-crop_normal.jpeg",
            "profile_banner_url": "https://pbs.twimg.com/profile_banners/14148390/1398269147",
            "profile_link_color": "911A1A",
            "profile_sidebar_border_color": "EEEEEE",
            "profile_sidebar_fill_color": "FFFFFF",
            "profile_text_color": "333333",
            "profile_use_background_image": 0,
            "has_extended_profile": 1,
            "default_profile": 0,
            "default_profile_image": 0,
            "following": 0,
            "follow_request_sent": 0,
            "notifications": 0,
            "translator_type": "none",
        },
        {
            "id": 22737278,
            "name": "David Roberts",
            "screen_name": "drvox",
            "location": "Seattle, WA",
            "description": "Seattleite transplanted from Tennessee; now blogging for http://Vox.com about energy politics. Climate hawk, deficit dove. Not a doctor.",
            "url": "http://www.vox.com/authors/david-roberts",
            "protected": 0,
            "followers_count": 132789,
            "friends_count": 2723,
            "listed_count": 4644,
            "created_at": "2009-03-04T05:14:12+00:00",
            "favourites_count": 26,
            "utc_offset": None,
            "time_zone": None,
            "geo_enabled": 0,
            "verified": 1,
            "statuses_count": 13887,
            "lang": None,
            "contributors_enabled": 0,
            "is_translator": 0,
            "is_translation_enabled": 0,
            "profile_background_color": "022330",
            "profile_background_image_url": "http://abs.twimg.com/images/themes/theme15/bg.png",
            "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme15/bg.png",
            "profile_background_tile": 0,
            "profile_image_url": "http://pbs.twimg.com/profile_images/551076081051004929/2i4QEfn-_normal.jpeg",
            "profile_image_url_https": "https://pbs.twimg.com/profile_images/551076081051004929/2i4QEfn-_normal.jpeg",
            "profile_banner_url": "https://pbs.twimg.com/profile_banners/22737278/1433745271",
            "profile_link_color": "0084B4",
            "profile_sidebar_border_color": "A8C7F7",
            "profile_sidebar_fill_color": "C0DFEC",
            "profile_text_color": "333333",
            "profile_use_background_image": 1,
            "has_extended_profile": 0,
            "default_profile": 0,
            "default_profile_image": 0,
            "following": 1,
            "follow_request_sent": 0,
            "notifications": 0,
            "translator_type": "none",
        },
    ] == user_rows


def test_tweets(db):
    tweet_rows = list(db["tweets"].rows)
    assert [
        {
            "id": 861696799362478100,
            "user": 14148390,
            "created_at": "2017-05-08T21:38:21+00:00",
            "full_text": "If you use Photos (mac) & Live Photos, run this command to generate a lovely sound collage of where you’ve been https://gist.github.com/bwhitman/5be2f905556a25145dbac74fe4080739",
            "retweeted_status": None,
            "quoted_status": None,
            "place": None,
            "source": "e6528b505bcfd811fdd40ff2d46665dbccba2024",
            "truncated": 0,
            "display_text_range": "[0, 139]",
            "in_reply_to_status_id": None,
            "in_reply_to_user_id": None,
            "in_reply_to_screen_name": None,
            "geo": None,
            "coordinates": None,
            "contributors": None,
            "is_quote_status": 0,
            "retweet_count": 14,
            "favorite_count": 57,
            "favorited": 0,
            "retweeted": 0,
            "possibly_sensitive": 0,
            "lang": "en",
        },
        {
            "id": 1168529001599533000,
            "user": 12497,
            "created_at": "2019-09-02T14:19:58+00:00",
            "full_text": "Finally got around to running this script. It is BRILLIANT - it produces a concatenated .wav file of the audio from every live photo you've ever taken.\n\nNeeds quite a lot of disk space to run - the /tmp/picblast folder can take multiple GB https://twitter.com/bwhitman/status/861696799362478085",
            "retweeted_status": None,
            "quoted_status": 861696799362478100,
            "place": None,
            "source": "1f89d6a41b1505a3071169f8d0d028ba9ad6f952",
            "truncated": 0,
            "display_text_range": "[0, 239]",
            "in_reply_to_status_id": None,
            "in_reply_to_user_id": None,
            "in_reply_to_screen_name": None,
            "geo": None,
            "coordinates": None,
            "contributors": None,
            "is_quote_status": 1,
            "retweet_count": 4,
            "favorite_count": 31,
            "favorited": 0,
            "retweeted": 0,
            "possibly_sensitive": 0,
            "lang": "en",
        },
        {
            "id": 1169196446043664400,
            "user": 12497,
            "created_at": "2019-09-04T10:32:10+00:00",
            "full_text": "@scientiffic @Wikipedia @unsplash @cagarrity The @inaturalist API is amazingly powerful and fun with no auth and no rate limit. We used it to build http://www.owlsnearme.com - see also @Natbat's great tutorial on using it with @observablehq https://24ways.org/2018/observable-notebooks-and-inaturalist/",
            "retweeted_status": None,
            "quoted_status": None,
            "place": "01a9a39529b27f36",
            "source": None,
            "truncated": 0,
            "display_text_range": "[45, 262]",
            "in_reply_to_status_id": "1169079390577320000",
            "in_reply_to_user_id": "82016165",
            "in_reply_to_screen_name": "scientiffic",
            "geo": None,
            "coordinates": None,
            "contributors": None,
            "is_quote_status": 0,
            "retweet_count": 0,
            "favorite_count": 2,
            "favorited": 0,
            "retweeted": 0,
            "possibly_sensitive": 0,
            "lang": "en",
        },
        {
            "id": 1169242008432644000,
            "user": 22737278,
            "created_at": "2019-09-04T13:33:12+00:00",
            "full_text": "My new post: an explainer on “carbon capture & utilization” (CCU). CO2 captured from waste gases or the ambient air can be used to make valuable products. Could CCU help the carbon capture industry scale up? https://www.vox.com/energy-and-environment/2019/9/4/20829431/climate-change-carbon-capture-utilization-sequestration-ccu-ccs?utm_campaign=drvox&utm_content=chorus&utm_medium=social&utm_source=twitter",
            "retweeted_status": None,
            "quoted_status": None,
            "place": None,
            "source": "942cfc2bf9f290ddbe3d78f1907dc084a00ed23f",
            "truncated": 0,
            "display_text_range": "[0, 235]",
            "in_reply_to_status_id": None,
            "in_reply_to_user_id": None,
            "in_reply_to_screen_name": None,
            "geo": None,
            "coordinates": None,
            "contributors": None,
            "is_quote_status": 0,
            "retweet_count": 42,
            "favorite_count": 86,
            "favorited": 1,
            "retweeted": 1,
            "possibly_sensitive": 0,
            "lang": "en",
        },
        {
            "id": 1169246717864136700,
            "user": 12497,
            "created_at": "2019-09-04T13:51:55+00:00",
            "full_text": "RT @drvox: My new post: an explainer on “carbon capture & utilization” (CCU). CO2 captured from waste gases or the ambient air can be used…",
            "retweeted_status": 1169242008432644000,
            "quoted_status": None,
            "place": None,
            "source": "95f3aaaddaa45937ac94765e0ddb68ba2be92d20",
            "truncated": 0,
            "display_text_range": "[0, 143]",
            "in_reply_to_status_id": None,
            "in_reply_to_user_id": None,
            "in_reply_to_screen_name": None,
            "geo": None,
            "coordinates": None,
            "contributors": None,
            "is_quote_status": 0,
            "retweet_count": 42,
            "favorite_count": 0,
            "favorited": 1,
            "retweeted": 1,
            "possibly_sensitive": None,
            "lang": "en",
        },
    ] == tweet_rows


def test_sources(db):
    source_rows = list(db["sources"].rows)
    assert [
        {
            "id": "95f3aaaddaa45937ac94765e0ddb68ba2be92d20",
            "name": "Twitter for iPhone",
            "url": "http://twitter.com/download/iphone",
        },
        {
            "id": "942cfc2bf9f290ddbe3d78f1907dc084a00ed23f",
            "name": "Vox Media",
            "url": "http://www.voxmedia.com",
        },
        {
            "id": "1f89d6a41b1505a3071169f8d0d028ba9ad6f952",
            "name": "Twitter Web App",
            "url": "https://mobile.twitter.com",
        },
        {
            "id": "e6528b505bcfd811fdd40ff2d46665dbccba2024",
            "name": "Twitter for Mac",
            "url": "http://itunes.apple.com/us/app/twitter/id409789998?mt=12",
        },
    ] == source_rows


def test_places(db):
    place_rows = list(db["places"].rows)
    assert [
        {
            "id": "01a9a39529b27f36",
            "url": "https://api.twitter.com/1.1/geo/id/01a9a39529b27f36.json",
            "place_type": "city",
            "name": "Manhattan",
            "full_name": "Manhattan, NY",
            "country_code": "US",
            "country": "United States",
            "contained_within": "[]",
            "bounding_box": '{"type": "Polygon", "coordinates": [[[-74.026675, 40.683935], [-73.910408, 40.683935], [-73.910408, 40.877483], [-74.026675, 40.877483]]]}',
            "attributes": "{}",
        }
    ] == place_rows


def test_media(db):
    media_rows = list(db["media"].rows)
    media_tweets_rows = list(db["media_tweets"].rows)
    assert [
        {
            "id": 504727051174031360,
            "id_str": "504727051174031361",
            "indices": "[116, 138]",
            "media_url": "http://pbs.twimg.com/media/BwEmlDHCMAEGwu_.jpg",
            "media_url_https": "https://pbs.twimg.com/media/BwEmlDHCMAEGwu_.jpg",
            "url": "http://t.co/uavPaDsvNe",
            "display_url": "pic.twitter.com/uavPaDsvNe",
            "expanded_url": "https://twitter.com/UpturnedBathtub/status/504727120812453889/photo/1",
            "type": "photo",
            "sizes": '{"thumb": {"w": 150, "h": 150, "resize": "crop"}, "large": {"w": 1024, "h": 768, "resize": "fit"}, "medium": {"w": 1024, "h": 768, "resize": "fit"}, "small": {"w": 680, "h": 510, "resize": "fit"}}',
        }
    ] == media_rows
    assert [
        {"media_id": 504727051174031360, "tweets_id": 1169196446043664400}
    ] == media_tweets_rows
