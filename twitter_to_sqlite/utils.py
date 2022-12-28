import click
import datetime
import html
import json
import pathlib
import re
import sqlite3
import time
import urllib.parse
import zipfile

from dateutil import parser
from requests import Session
from requests.auth import AuthBase
from requests_oauthlib import OAuth1Session
import sqlite_utils

# Twitter API error codes
RATE_LIMIT_ERROR_CODE = 88

SINCE_ID_TYPES = {
    "user": 1,
    "home": 2,
    "mentions": 3,
    "search": 4,
}
COUNT_HISTORY_TYPES = {
    "followers": 1,
    "friends": 2,
    "listed": 3,
    # Don't track these - they're uninteresting and really noisy in terms
    # of writing new rows to the count_history table:
    # "favourites": 4,
    # "statuses": 5,
}

source_re = re.compile('<a href="(?P<url>.*?)".*?>(?P<name>.*?)</a>')


class UserDoesNotExist(click.ClickException):
    def __init__(self, identifier):
        super().__init__("User '{}' does not exist".format(identifier))


def open_database(db_path):
    db = sqlite_utils.Database(db_path)
    # Only run migrations if this is an existing DB (has tables)
    if db.tables:
        migrate(db)
    return db


def migrate(db):
    from twitter_to_sqlite.migrations import MIGRATIONS

    if "migrations" not in db.table_names():
        db["migrations"].create({"name": str, "applied": str}, pk="name")
    applied_migrations = {
        m[0] for m in db.conn.execute("select name from migrations").fetchall()
    }
    for migration in MIGRATIONS:
        name = migration.__name__
        if name in applied_migrations:
            continue
        migration(db)
        db["migrations"].insert(
            {"name": name, "applied": datetime.datetime.utcnow().isoformat()}
        )


class BearerTokenAuth(AuthBase):
    def __init__(self, bearer_token):
        self.bearer_token = bearer_token

    def __call__(self, r):
        r.headers["Authorization"] = "Bearer " + self.bearer_token
        return r


def session_for_auth(auth):
    if "bearer_token" in auth:
        session = Session()
        session.auth = BearerTokenAuth(auth["bearer_token"])
        return session
    else:
        return OAuth1Session(
            client_key=auth["api_key"],
            client_secret=auth["api_secret_key"],
            resource_owner_key=auth["access_token"],
            resource_owner_secret=auth["access_token_secret"],
        )


def fetch_user_list_chunks(
    session, user_id=None, screen_name=None, sleep=61, noun="followers"
):
    cursor = -1
    users = []
    while cursor:
        headers, body = fetch_user_list(session, cursor, user_id, screen_name, noun)
        yield body["users"]
        cursor = body["next_cursor"]
        if not cursor:
            break
        time.sleep(sleep)  # Rate limit = 15 per 15 minutes!


def fetch_user_list(session, cursor, user_id=None, screen_name=None, noun="followers"):
    args = user_args(user_id, screen_name)
    args.update({"count": 200, "cursor": cursor})
    r = session.get(
        "https://api.twitter.com/1.1/{}/list.json?".format(noun)
        + urllib.parse.urlencode(args)
    )
    return r.headers, r.json()


def fetch_lists(db, session, user_id=None, screen_name=None):
    lists_url = "https://api.twitter.com/1.1/lists/ownerships.json"
    args = user_args(user_id, screen_name)
    args["count"] = 1000
    fetched_lists = []
    # For the moment we don't paginate
    for list_row in session.get(lists_url, params=args).json()["lists"]:
        del list_row["id_str"]
        user = list_row.pop("user")
        save_users(db, [user])
        list_row["user"] = user["id"]
        list_row["created_at"] = parser.parse(list_row["created_at"])
        fetched_lists.append(list_row)
    db["lists"].insert_all(fetched_lists, pk="id", foreign_keys=("user",), replace=True)
    return fetched_lists


def get_profile(db, session, user_id=None, screen_name=None):
    if not (user_id or screen_name):
        profile = session.get(
            "https://api.twitter.com/1.1/account/verify_credentials.json"
        ).json()
    else:
        args = user_args(user_id, screen_name)
        url = "https://api.twitter.com/1.1/users/show.json"
        if args:
            url += "?" + urllib.parse.urlencode(args)
        response = session.get(url)
        if response.status_code == 404:
            raise UserDoesNotExist(screen_name or user_id)
        profile = response.json()
    save_users(db, [profile])
    return profile


def fetch_timeline(
    session,
    url,
    db,
    args=None,
    sleep=1,
    stop_after=None,
    key=None,
    since_id=None,
    since=False,
    since_type=None,
    since_key=None,
):
    # See https://developer.twitter.com/en/docs/tweets/timelines/guides/working-with-timelines
    if since and since_id:
        raise click.ClickException("Use either --since or --since_id, not both")

    since_type_id = None
    last_since_id = None
    if since_type is not None:
        assert since_key is not None
        since_type_id = SINCE_ID_TYPES[since_type]
        # Figure out the last since_id in case we need it
        try:
            last_since_id = db.conn.execute(
                """
                select since_id from since_ids
                where type = ? and key = ?
                """,
                [since_type_id, since_key],
            ).fetchall()[0][0]
        except (IndexError, sqlite3.OperationalError):
            pass

    if since:
        # Load since_id from database
        since_id = last_since_id

    args = dict(args or {})
    args["count"] = 200
    if stop_after is not None:
        args["count"] = stop_after
    if since_id:
        args["since_id"] = since_id
    args["tweet_mode"] = "extended"
    min_seen_id = None
    num_rate_limit_errors = 0
    while True:
        if min_seen_id is not None:
            args["max_id"] = min_seen_id - 1
        response = session.get(url, params=args)
        tweets = response.json()
        if "errors" in tweets:
            # Was it a rate limit error? If so sleep and try again
            if RATE_LIMIT_ERROR_CODE == tweets["errors"][0]["code"]:
                num_rate_limit_errors += 1
                assert num_rate_limit_errors < 5, "More than 5 rate limit errors"
                print(
                    "Rate limit exceeded - will sleep 15s and try again {}".format(
                        repr(response.headers)
                    )
                )
                time.sleep(15)
                continue
            else:
                raise Exception(str(tweets["errors"]))
        if key is not None:
            tweets = tweets[key]
        if not tweets:
            break
        for tweet in tweets:
            yield tweet
        min_seen_id = min(t["id"] for t in tweets)
        max_seen_id = max(t["id"] for t in tweets)
        if last_since_id is not None:
            max_seen_id = max((last_since_id, max_seen_id))
            last_since_id = max_seen_id
        if since_type_id is not None and since_key is not None:
            db["since_ids"].insert(
                {
                    "type": since_type_id,
                    "key": since_key,
                    "since_id": max_seen_id,
                },
                replace=True,
            )
        if stop_after is not None:
            break
        time.sleep(sleep)


def fetch_user_timeline(
    session,
    db,
    user_id=None,
    screen_name=None,
    stop_after=None,
    since_id=None,
    since=False,
):
    args = user_args(user_id, screen_name)
    yield from fetch_timeline(
        session,
        "https://api.twitter.com/1.1/statuses/user_timeline.json",
        db,
        args,
        sleep=1,
        stop_after=stop_after,
        since_id=since_id,
        since_type="user",
        since_key="id:{}".format(user_id) if user_id else screen_name,
        since=since,
    )


def fetch_favorites(session, db, user_id=None, screen_name=None, stop_after=None):
    args = user_args(user_id, screen_name)
    # Rate limit 75/15 mins = 5/minute = every 12 seconds
    sleep = 12
    yield from fetch_timeline(
        session,
        "https://api.twitter.com/1.1/favorites/list.json",
        db,
        args,
        sleep=sleep,
        stop_after=stop_after,
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
    tweet["full_text"] = html.unescape(
        expand_entities(tweet["full_text"], tweet.pop("entities"))
    )
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
    if "sources" not in table_names:
        db["sources"].create({"id": str, "name": str, "url": str}, pk="id")
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
                "source": str,
            },
            pk="id",
            foreign_keys=(
                ("user", "users", "id"),
                ("place", "places", "id"),
                ("source", "sources", "id"),
            ),
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
    # Ensure following has indexes
    following_indexes = {tuple(i.columns) for i in db["following"].indexes}
    if ("followed_id",) not in following_indexes:
        db["following"].create_index(["followed_id"])
    if ("follower_id",) not in following_indexes:
        db["following"].create_index(["follower_id"])

    # Tables for tracking --since
    if "since_ids" not in table_names:
        db["since_id_types"].create(
            {
                "id": int,
                "name": str,
            },
            pk="id",
        )
        db["since_id_types"].insert_all(
            [{"id": id, "name": name} for name, id in SINCE_ID_TYPES.items()]
        )
        db["since_ids"].create(
            {"type": int, "key": str, "since_id": int},
            pk=("type", "key"),
            foreign_keys=(("type", "since_id_types", "id"),),
        )

    # Tables for recording history of user follower counts etc
    if "count_history" not in table_names:
        db["count_history_types"].create(
            {
                "id": int,
                "name": str,
            },
            pk="id",
        )
        db["count_history_types"].insert_all(
            [{"id": id, "name": name} for name, id in COUNT_HISTORY_TYPES.items()]
        )
        db["count_history"].create(
            {"type": int, "user": int, "datetime": str, "count": int},
            pk=("type", "user", "datetime"),
            foreign_keys=(
                ("type", "count_history_types", "id"),
                ("user", "users", "id"),
            ),
        )


def save_tweets(db, tweets, favorited_by=None):
    ensure_tables(db)
    for tweet in tweets:
        transform_tweet(tweet)
        user = tweet.pop("user")
        transform_user(user)
        tweet["user"] = user["id"]
        tweet["source"] = extract_and_save_source(db, tweet["source"])
        if tweet.get("place"):
            db["places"].insert(tweet["place"], pk="id", alter=True, replace=True)
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
        db["users"].insert(user, pk="id", alter=True, replace=True)
        save_user_counts(db, user)
        table = db["tweets"].insert(tweet, pk="id", alter=True, replace=True)
        if favorited_by is not None:
            db["favorited_by"].insert(
                {"tweet": tweet["id"], "user": favorited_by},
                pk=("user", "tweet"),
                foreign_keys=("tweet", "user"),
                replace=True,
            )
        if extended_entities and extended_entities.get("media"):
            for media in extended_entities["media"]:
                # TODO: Remove this line when .m2m() grows alter=True
                db["media"].insert(media, pk="id", alter=True, replace=True)
                table.m2m("media", media, pk="id")


def save_users(db, users, followed_id=None, follower_id=None):
    assert not (followed_id and follower_id)
    ensure_tables(db)
    for user in users:
        transform_user(user)
    db["users"].insert_all(users, pk="id", alter=True, replace=True)
    for user in users:
        save_user_counts(db, user)
    if followed_id or follower_id:
        first_seen = datetime.datetime.utcnow().isoformat()
        db["following"].insert_all(
            (
                {
                    "followed_id": followed_id or user["id"],
                    "follower_id": follower_id or user["id"],
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
    db["lists"].insert(data, pk="id", foreign_keys=("user",), replace=True)
    # Now fetch the members
    url = "https://api.twitter.com/1.1/lists/members.json"
    cursor = -1
    while cursor:
        args.update({"count": 5000, "cursor": cursor})
        body = session.get(url, params=args).json()
        users = body["users"]
        save_users(db, users)
        db["list_members"].insert_all(
            ({"list": list_id, "user": user["id"]} for user in users),
            pk=("list", "user"),
            foreign_keys=("list", "user"),
            replace=True,
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


def read_archive_js(filepath):
    "Open zip file, return (filename, content) for all .js"
    zf = zipfile.ZipFile(filepath)
    for zi in zf.filelist:
        # Ignore files in a assets dir -- these are for Twitter's archive
        # browser thingie -- and only use final filenames since some archives
        # appear to put data in a data/ subdir, which can screw up the filename
        # -> importer mapping.
        if zi.filename.endswith(".js") and not zi.filename.startswith("assets/"):
            yield pathlib.Path(zi.filename).name, zf.open(zi.filename).read()


def extract_and_save_source(db, source):
    if not source:
        return None
    m = source_re.match(source)
    details = m.groupdict()
    return db["sources"].insert(details, hash_id="id", replace=True).last_pk


def save_user_counts(db, user):
    for type_name, type_id in COUNT_HISTORY_TYPES.items():
        previous_count = None
        try:
            previous_count = db.conn.execute(
                """
                select count from count_history
                where type = ? and user = ?
                order by datetime desc limit 1
                """,
                [type_id, user["id"]],
            ).fetchall()[0][0]
        except IndexError:
            pass
        current_count = user["{}_count".format(type_name)]
        if current_count != previous_count:
            db["count_history"].insert(
                {
                    "type": type_id,
                    "user": user["id"],
                    "datetime": datetime.datetime.utcnow().isoformat().split(".")[0]
                    + "+00:00",
                    "count": current_count,
                },
                replace=True,
            )
