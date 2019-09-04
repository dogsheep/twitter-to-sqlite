from requests_oauthlib import OAuth1Session
from dateutil import parser
import datetime
import time
import json
import urllib.parse


def session_for_auth(auth):
    return OAuth1Session(
        client_key=auth["api_key"],
        client_secret=auth["api_secret_key"],
        resource_owner_key=auth["access_token"],
        resource_owner_secret=auth["access_token_secret"],
    )


def fetch_follower_chunks(session, user_id, screen_name, sleep=61):
    cursor = -1
    users = []
    while cursor:
        headers, body = fetch_followers(session, cursor, user_id, screen_name)
        yield body["users"]
        cursor = body["next_cursor"]
        if not cursor:
            break
        time.sleep(sleep)  # Rate limit = 15 per 15 minutes!


def fetch_followers(session, cursor, user_id, screen_name):
    args = user_args(user_id, screen_name)
    args.update({"count": 200, "cursor": cursor})
    r = session.get(
        "https://api.twitter.com/1.1/followers/list.json?"
        + urllib.parse.urlencode(args)
    )
    return r.headers, r.json()


def get_profile(session, user_id, screen_name):
    if not (user_id or screen_name):
        return session.get(
            "https://api.twitter.com/1.1/account/verify_credentials.json"
        ).json()
    args = user_args(user_id, screen_name)
    url = "https://api.twitter.com/1.1/users/show.json"
    if args:
        url += "?" + urllib.parse.urlencode(args)
    return session.get(url).json()


def fetch_timeline(session, url, args, sleep=1):
    # See https://developer.twitter.com/en/docs/tweets/timelines/guides/working-with-timelines
    args = dict(args)
    args["count"] = 200
    args["tweet_mode"] = "extended"
    min_seen_id = None
    while True:
        if min_seen_id is not None:
            args["max_id"] = min_seen_id - 1
        tweets = session.get(url, params=args).json()
        if not tweets:
            break
        for tweet in tweets:
            yield tweet
        min_seen_id = min(t["id"] for t in tweets)
        time.sleep(sleep)


def fetch_user_timeline(session, user_id, screen_name):
    args = user_args(user_id, screen_name)
    yield from fetch_timeline(
        session,
        "https://api.twitter.com/1.1/statuses/user_timeline.json",
        args,
        sleep=1,
    )


def fetch_favorites(session, user_id, screen_name):
    args = user_args(user_id, screen_name)
    # Rate limit 75/15 mins = 5/minute = every 12 seconds
    sleep = 12
    yield from fetch_timeline(
        session, "https://api.twitter.com/1.1/favorites/list.json", args, sleep=sleep
    )


def user_args(user_id, screen_name):
    args = {}
    if user_id:
        args["user_id"] = user_id
    if screen_name:
        args["screen_name"] = screen_name
    return args


def expand_entities(s, entities):
    for _, ents in entities.items():
        for ent in ents:
            if "url" in ent:
                replacement = ent["expanded_url"] or ent["url"]
                s = s.replace(ent["url"], replacement)
    return s


def transform_user(user):
    user["created_at"] = parser.parse(user["created_at"])
    if user["description"] and "description" in user["entities"]:
        user["description"] = expand_entities(
            user["description"], user["entities"]["description"]
        )
    if user["url"] and "url" in user["entities"]:
        user["url"] = expand_entities(user["url"], user["entities"]["url"])
    user.pop("entities", None)
    user.pop("status", None)
    to_remove = [k for k in user if k.endswith("_str")]
    for key in to_remove:
        del user[key]


def transform_tweet(tweet):
    tweet["full_text"] = expand_entities(tweet["full_text"], tweet.pop("entities"))
    to_remove = [k for k in tweet if k.endswith("_str")] + [
        "quoted_status_id",
        "quoted_status_permalink",
    ]
    for key in to_remove:
        if key in tweet:
            del tweet[key]
    tweet["created_at"] = parser.parse(tweet["created_at"]).isoformat()


def ensure_tables(db):
    table_names = set(db.table_names())
    if "users" not in table_names:
        db["users"].create(
            {
                "id": int,
                "screen_name": str,
                "name": str,
                "description": str,
                "location": str,
            },
            pk="id",
        )
        db["users"].enable_fts(
            ["name", "screen_name", "description", "location"], create_triggers=True
        )
    if "tweets" not in table_names:
        db["tweets"].create(
            {
                "id": int,
                "user": int,
                "created_at": str,
                "full_text": str,
                "retweeted_status": int,
                "quoted_status": int,
            },
            pk="id",
            foreign_keys=(("user", "users", "id"),),
        )
        db["tweets"].add_foreign_key("retweeted_status", "tweets")
        db["tweets"].add_foreign_key("quoted_status", "tweets")
    if "following" not in table_names:
        db["following"].create(
            {"followed_id": int, "follower_id": int, "first_seen": str},
            pk=("followed_id", "follower_id"),
            foreign_keys=(
                ("followed_id", "users", "id"),
                ("follower_id", "users", "id"),
            ),
        )


def save_tweets(db, tweets):
    tweets = list(tweets)
    ensure_tables(db)
    for tweet in tweets:
        transform_tweet(tweet)
        user = tweet.pop("user")
        transform_user(user)
        tweet["user"] = user["id"]
        # Deal with nested retweeted_status / quoted_status
        nested = []
        for tweet_key in ("quoted_status", "retweeted_status"):
            if tweet.get(tweet_key):
                nested.append(tweet[tweet_key])
                tweet[tweet_key] = tweet[tweet_key]["id"]
        if nested:
            save_tweets(db, nested)
        db["users"].upsert(user, pk="id", alter=True)
    if tweets:
        db["tweets"].upsert_all(tweets, pk="id", alter=True)


def save_users(db, users, followed_id=None):
    ensure_tables(db)
    for user in users:
        transform_user(user)
    db["users"].upsert_all(users, pk="id", alter=True)
    if followed_id:
        first_seen = datetime.datetime.utcnow().isoformat()
        db["following"].insert_all(
            (
                {
                    "followed_id": followed_id,
                    "follower_id": user["id"],
                    "first_seen": first_seen,
                }
                for user in users
            ),
            ignore=True,
        )
