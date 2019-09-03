from requests_oauthlib import OAuth1Session
import time
import urllib.parse


def session_for_auth(auth):
    return OAuth1Session(
        client_key=auth["api_key"],
        client_secret=auth["api_secret_key"],
        resource_owner_key=auth["access_token"],
        resource_owner_secret=auth["access_token_secret"],
    )


def fetch_follower_chunks(session, user_id, screen_name):
    cursor = -1
    users = []
    while cursor:
        headers, body = fetch_followers(session, cursor, user_id, screen_name)
        yield body["users"]
        cursor = body["next_cursor"]
        time.sleep(61)  # Rate limit = 15 per 15 minutes!


def fetch_followers(session, cursor, user_id, screen_name):
    args = {"count": 200, "cursor": cursor}
    if user_id:
        args["user_id"] = user_id
    if screen_name:
        args["screen_name"] = screen_name
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
    args = {}
    if user_id:
        args["user_id"] = user_id
    if screen_name:
        args["screen_name"] = screen_name
    url = "https://api.twitter.com/1.1/users/show.json"
    if args:
        url += "?" + urllib.parse.urlencode(args)
    return session.get(url).json()
