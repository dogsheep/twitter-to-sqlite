from requests_oauthlib import OAuth1Session
from dateutil import parser
import datetime
import time
import pathlib
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


def fetch_timeline(session, url, args, sleep=1, stop_after=None):
    # See https://developer.twitter.com/en/docs/tweets/timelines/guides/working-with-timelines
    args = dict(args)
    args["count"] = 200
    if stop_after is not None:
        args["count"] = stop_after
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
        if stop_after is not None:
            break
        time.sleep(sleep)


def fetch_user_timeline(session, user_id, screen_name, stop_after=None):
    args = user_args(user_id, screen_name)
    yield from fetch_timeline(
        session,
        "https://api.twitter.com/1.1/statuses/user_timeline.json",
        args,
        sleep=1,
        stop_after=stop_after,
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
    if user["description"] and "description" in user.get("entities", {}):
        user["description"] = expand_entities(
            user["description"], user["entities"]["description"]
        )
    if user["url"] and "url" in user.get("entities", {}):
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
    if "places" not in table_names:
        db["places"].create({"id": str}, pk="id")
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
                "place": str,
            },
            pk="id",
            foreign_keys=(("user", "users", "id"), ("place", "places", "id")),
        )
        db["tweets"].enable_fts(["full_text"], create_triggers=True)
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
    ensure_tables(db)
    for tweet in tweets:
        transform_tweet(tweet)
        user = tweet.pop("user")
        transform_user(user)
        tweet["user"] = user["id"]
        if tweet.get("place"):
            db["places"].upsert(tweet["place"], pk="id", alter=True)
            tweet["place"] = tweet["place"]["id"]
        # extended_entities contains media
        extended_entities = tweet.pop("extended_entities", None)
        # Deal with nested retweeted_status / quoted_status
        nested = []
        for tweet_key in ("quoted_status", "retweeted_status"):
            if tweet.get(tweet_key):
                nested.append(tweet[tweet_key])
                tweet[tweet_key] = tweet[tweet_key]["id"]
        if nested:
            save_tweets(db, nested)
        db["users"].upsert(user, pk="id", alter=True)
        table = db["tweets"].upsert(tweet, pk="id", alter=True)
        if extended_entities and extended_entities.get("media"):
            for media in extended_entities["media"]:
                # TODO: Remove this line when .m2m() grows alter=True
                db["media"].upsert(media, pk="id", alter=True)
                table.m2m("media", media, pk="id")


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


def fetch_user_batches(session, ids_or_screen_names, use_ids=False, sleep=1):
    # Yields lists of up to 70 users (tried 100 but got this error:
    # # {'code': 18, 'message': 'Too many terms specified in query.'} )
    batches = []
    batch = []
    for id in ids_or_screen_names:
        batch.append(id)
        if len(batch) == 70:
            batches.append(batch)
            batch = []
    if batch:
        batches.append(batch)
    url = "https://api.twitter.com/1.1/users/lookup.json"
    for batch in batches:
        if use_ids:
            args = {"user_id": ",".join(map(str, batch))}
        else:
            args = {"screen_name": ",".join(batch)}
        users = session.get(url, params=args).json()
        yield users
        time.sleep(sleep)


def fetch_status_batches(session, tweet_ids, sleep=1):
    # Yields lists of up to 100 tweets
    batches = []
    batch = []
    for id in tweet_ids:
        batch.append(id)
        if len(batch) == 100:
            batches.append(batch)
            batch = []
    if batch:
        batches.append(batch)
    url = "https://api.twitter.com/1.1/statuses/lookup.json"
    for batch in batches:
        args = {"id": ",".join(map(str, batch)), "tweet_mode": "extended"}
        tweets = session.get(url, params=args).json()
        yield tweets
        time.sleep(sleep)


def resolve_identifiers(db, identifiers, attach, sql):
    if sql:
        if attach:
            for filepath in attach:
                if ":" in filepath:
                    alias, filepath = filepath.split(":", 1)
                else:
                    alias = filepath.split("/")[-1].split(".")[0]
                attach_sql = """
                    ATTACH DATABASE '{}' AS [{}];
                """.format(
                    str(pathlib.Path(filepath).resolve()), alias
                )
                db.conn.execute(attach_sql)
        sql_identifiers = [r[0] for r in db.conn.execute(sql).fetchall()]
    else:
        sql_identifiers = []
    return list(identifiers) + sql_identifiers


def fetch_and_save_list(db, session, identifier, identifier_is_id=False):
    show_url = "https://api.twitter.com/1.1/lists/show.json"
    args = {}
    if identifier_is_id:
        args["list_id"] = identifier
    else:
        screen_name, slug = identifier.split("/")
        args.update({"owner_screen_name": screen_name, "slug": slug})
    # First fetch the list details
    data = session.get(show_url, params=args).json()
    list_id = data["id"]
    del data["id_str"]
    user = data.pop("user")
    save_users(db, [user])
    data["user"] = user["id"]
    data["created_at"] = parser.parse(data["created_at"])
    db["lists"].upsert(data, pk="id", foreign_keys=("user",))
    # Now fetch the members
    url = "https://api.twitter.com/1.1/lists/members.json"
    cursor = -1
    while cursor:
        args.update({"count": 5000, "cursor": cursor})
        body = session.get(url, params=args).json()
        users = body["users"]
        save_users(db, users)
        db["list_members"].upsert_all(
            ({"list": list_id, "user": user["id"]} for user in users),
            pk=("list", "user"),
            foreign_keys=("list", "user"),
        )
        cursor = body["next_cursor"]
        if not cursor:
            break
        time.sleep(1)  # Rate limit = 900 per 15 minutes


def cursor_paginate(session, url, args, key, page_size=200, sleep=None):
    "Execute cursor pagination, yelding 'key' for each page"
    args = dict(args)
    args["page_size"] = page_size
    cursor = -1
    while cursor:
        args["cursor"] = cursor
        r = session.get(url, params=args)
        raise_if_error(r)
        body = r.json()
        yield body[key]
        cursor = body["next_cursor"]
        if not cursor:
            break
        if sleep is not None:
            time.sleep(sleep)


class TwitterApiError(Exception):
    def __init__(self, headers, body):
        self.headers = headers
        self.body = body

    def __repr__(self):
        return "{}: {}".format(self.body, self.headers)


def raise_if_error(r):
    if "errors" in r.json():
        raise TwitterApiError(r.headers, r.json()["errors"])


def stream_filter(session, track=None, follow=None, locations=None, language=None):
    session.stream = True
    args = {"tweet_mode": "extended"}
    for key, value in (
        ("track", track),
        ("follow", follow),
        ("locations", locations),
        ("language", language),
    ):
        if value is None:
            continue
        if not isinstance(value, str):
            value = ",".join(map(str, value))
        args[key] = value
    while True:
        response = session.post(
            "https://stream.twitter.com/1.1/statuses/filter.json", params=args
        )
        for line in response.iter_lines(chunk_size=10000):
            if line.strip().startswith(b"{"):
                tweet = json.loads(line)
                # Only yield tweet if it has an 'id' and 'created_at'
                # - otherwise it's probably a maintenance message, see
                # https://developer.twitter.com/en/docs/tweets/filter-realtime/overview/statuses-filter
                if "id" in tweet and "created_at" in tweet:
                    # 'Fix' weird tweets from streaming API
                    fix_streaming_tweet(tweet)
                    yield tweet
                else:
                    print(tweet)
        time.sleep(1)


def fix_streaming_tweet(tweet):
    if "extended_tweet" in tweet:
        tweet.update(tweet.pop("extended_tweet"))
    if "full_text" not in tweet:
        tweet["full_text"] = tweet["text"]
    if "retweeted_status" in tweet:
        fix_streaming_tweet(tweet["retweeted_status"])
    if "quoted_status" in tweet:
        fix_streaming_tweet(tweet["quoted_status"])


def user_ids_for_screen_names(db, screen_names):
    sql = "select id from users where lower(screen_name) in ({})".format(
        ", ".join(["?"] * len(screen_names))
    )
    return [
        r[0] for r in db.conn.execute(sql, [s.lower() for s in screen_names]).fetchall()
    ]
