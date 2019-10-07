import click
import datetime
import os
import sqlite_utils
import time
import json
from twitter_to_sqlite import utils


def add_identifier_options(subcommand):
    for decorator in reversed(
        (
            click.argument("identifiers", type=str, nargs=-1),
            click.option(
                "--attach",
                type=click.Path(
                    file_okay=True, dir_okay=False, allow_dash=False, exists=True
                ),
                multiple=True,
                help="Additional database file to attach",
            ),
            click.option("--sql", help="SQL query to fetch identifiers to use"),
        )
    ):
        subcommand = decorator(subcommand)
    return subcommand


@click.group()
@click.version_option()
def cli():
    "Save data from Twitter to a SQLite database"


@cli.command()
@click.argument("url")
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True, exists=True),
    default="auth.json",
    help="Path to auth.json token file",
)
def fetch(url, auth):
    "Make an authenticated request to the Twitter API"
    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    click.echo(json.dumps(session.get(url).json(), indent=4))


@cli.command()
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    default="auth.json",
    help="Path to save tokens to, defaults to auth.json",
)
def auth(auth):
    "Save authentication credentials to a JSON file"
    click.echo("Create an app here: https://developer.twitter.com/en/apps")
    click.echo("Then navigate to 'Keys and tokens' and paste in the following:")
    click.echo()
    api_key = click.prompt("API key")
    api_secret_key = click.prompt("API secret key")
    access_token = click.prompt("Access token")
    access_token_secret = click.prompt("Access token secret")
    open(auth, "w").write(
        json.dumps(
            {
                "api_key": api_key,
                "api_secret_key": api_secret_key,
                "access_token": access_token,
                "access_token_secret": access_token_secret,
            },
            indent=4,
        )
        + "\n"
    )


@cli.command()
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True, exists=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option("--user_id", help="Numeric user ID")
@click.option("--screen_name", help="Screen name")
@click.option("--silent", is_flag=True, help="Disable progress bar")
def followers(db_path, auth, user_id, screen_name, silent):
    "Save followers for specified user (defaults to authenticated user)"
    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    db = sqlite_utils.Database(db_path)
    fetched = []
    # Get the follower count, so we can have a progress bar
    count = 0

    profile = utils.get_profile(session, user_id, screen_name)
    screen_name = profile["screen_name"]
    user_id = profile["id"]

    def go(update):
        utils.save_users(db, [profile])
        for followers_chunk in utils.fetch_follower_chunks(
            session, user_id, screen_name
        ):
            fetched.extend(followers_chunk)
            utils.save_users(db, followers_chunk, followed_id=user_id)
            update(len(followers_chunk))

    if not silent:
        count = profile["followers_count"]
        with click.progressbar(
            length=count,
            label="Importing {:,} followers for @{}".format(count, screen_name),
        ) as bar:
            go(bar.update)
    else:
        go(lambda x: None)
    # open("/tmp/all.json", "w").write(json.dumps(fetched, indent=4))


@cli.command()
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True, exists=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option("--user_id", help="Numeric user ID")
@click.option("--screen_name", help="Screen name")
def favorites(db_path, auth, user_id, screen_name):
    "Save tweets favorited by specified user"
    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    db = sqlite_utils.Database(db_path)
    with click.progressbar(
        utils.fetch_favorites(session, user_id, screen_name),
        label="Importing favorites",
        show_pos=True,
    ) as bar:
        utils.save_tweets(db, bar)


@cli.command(name="user-timeline")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True, exists=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option("--stop_after", type=int, help="Only pull this number of recent tweets")
@click.option("--user_id", help="Numeric user ID")
@click.option("--screen_name", help="Screen name")
def user_timeline(db_path, auth, stop_after, user_id, screen_name):
    "Save tweets posted by specified user"
    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    profile = utils.get_profile(session, user_id, screen_name)
    db = sqlite_utils.Database(db_path)
    with click.progressbar(
        utils.fetch_user_timeline(session, user_id, screen_name, stop_after),
        length=profile["statuses_count"],
        label="Importing tweets",
        show_pos=True,
    ) as bar:
        # Save them 100 at a time
        chunk = []
        for tweet in bar:
            chunk.append(tweet)
            if len(chunk) >= 100:
                utils.save_tweets(db, chunk)
                chunk = []
        if chunk:
            utils.save_tweets(db, chunk)


@cli.command(name="users-lookup")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@add_identifier_options
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True, exists=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option("--ids", is_flag=True, help="Treat input as user IDs, not screen names")
def users_lookup(db_path, identifiers, attach, sql, auth, ids):
    "Fetch user accounts"
    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    db = sqlite_utils.Database(db_path)
    identifiers = utils.resolve_identifiers(db, identifiers, attach, sql)
    for batch in utils.fetch_user_batches(session, identifiers, ids):
        utils.save_users(db, batch)


@cli.command(name="statuses-lookup")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@add_identifier_options
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True, exists=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option(
    "--skip-existing", is_flag=True, help="Skip tweets that are already in the DB"
)
@click.option("--silent", is_flag=True, help="Disable progress bar")
def statuses_lookup(db_path, identifiers, attach, sql, auth, skip_existing, silent):
    "Fetch tweets by their IDs"
    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    db = sqlite_utils.Database(db_path)
    identifiers = utils.resolve_identifiers(db, identifiers, attach, sql)
    if skip_existing:
        existing_ids = set(
            r[0] for r in db.conn.execute("select id from tweets").fetchall()
        )
        identifiers = [i for i in identifiers if int(i) not in existing_ids]
    if silent:
        for batch in utils.fetch_status_batches(session, identifiers):
            utils.save_tweets(db, batch)
    else:
        # Do it with a progress bar
        count = len(identifiers)
        with click.progressbar(
            length=count,
            label="Importing {:,} tweet{}".format(count, "" if count == 1 else "s"),
        ) as bar:
            for batch in utils.fetch_status_batches(session, identifiers):
                utils.save_tweets(db, batch)
                bar.update(len(batch))


@cli.command(name="list-members")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("identifiers", type=str, nargs=-1)
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True, exists=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option(
    "--ids", is_flag=True, help="Treat input as list IDs, not user/slug strings"
)
def list_members(db_path, identifiers, auth, ids):
    "Fetch lists - accepts one or more screen_name/list_slug identifiers"
    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    db = sqlite_utils.Database(db_path)
    for identifier in identifiers:
        utils.fetch_and_save_list(db, session, identifier, ids)


@cli.command(name="followers-ids")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@add_identifier_options
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True, exists=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option(
    "--ids", is_flag=True, help="Treat input as list IDs, not user/slug strings"
)
@click.option(
    "--sleep", type=int, default=61, help="Seconds to sleep between API calls"
)
def followers_ids(db_path, identifiers, attach, sql, auth, ids, sleep):
    "Populate followers table with IDs of account followers"
    _shared_friends_ids_followers_ids(
        db_path,
        identifiers,
        attach,
        sql,
        auth,
        ids,
        sleep,
        api_url="https://api.twitter.com/1.1/followers/ids.json",
        first_key="followed_id",
        second_key="follower_id",
    )


@cli.command(name="friends-ids")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@add_identifier_options
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True, exists=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option(
    "--ids", is_flag=True, help="Treat input as list IDs, not user/slug strings"
)
@click.option(
    "--sleep", type=int, default=61, help="Seconds to sleep between API calls"
)
def friends_ids(db_path, identifiers, attach, sql, auth, ids, sleep):
    "Populate followers table with IDs of account friends"
    _shared_friends_ids_followers_ids(
        db_path,
        identifiers,
        attach,
        sql,
        auth,
        ids,
        sleep,
        api_url="https://api.twitter.com/1.1/friends/ids.json",
        first_key="follower_id",
        second_key="followed_id",
    )


@cli.command()
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("track", type=str, required=True, nargs=-1)
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True, exists=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option("--verbose", is_flag=True, help="Verbose mode: display every tweet")
def track(db_path, track, auth, verbose):
    "Experimental: Save tweets matching these keywords in real-time"
    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    db = sqlite_utils.Database(db_path)
    for tweet in utils.stream_filter(session, track=track):
        if verbose:
            print(json.dumps(tweet, indent=2))
        with db.conn:
            utils.save_tweets(db, [tweet])


@cli.command()
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@add_identifier_options
@click.option("--ids", is_flag=True, help="Treat input as user IDs, not screen names")
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True, exists=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option("--verbose", is_flag=True, help="Verbose mode: display every tweet")
def follow(db_path, identifiers, attach, sql, ids, auth, verbose):
    "Experimental: Follow these Twitter users and save tweets in real-time"
    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    db = sqlite_utils.Database(db_path)
    identifiers = utils.resolve_identifiers(db, identifiers, attach, sql)
    # Make sure we have saved these users to the database
    for batch in utils.fetch_user_batches(session, identifiers, ids):
        utils.save_users(db, batch)
    # Ensure we have user IDs, not screen names
    if ids:
        follow = identifiers
    else:
        follow = utils.user_ids_for_screen_names(db, identifiers)
    # Start streaming:
    for tweet in utils.stream_filter(session, follow=follow):
        if verbose:
            print(json.dumps(tweet, indent=2))
        with db.conn:
            utils.save_tweets(db, [tweet])


def _shared_friends_ids_followers_ids(
    db_path, identifiers, attach, sql, auth, ids, sleep, api_url, first_key, second_key
):
    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    db = sqlite_utils.Database(db_path)
    identifiers = utils.resolve_identifiers(db, identifiers, attach, sql)
    for identifier in identifiers:
        # Make sure this user is saved
        arg_user_id = identifier if ids else None
        arg_screen_name = None if ids else identifier
        profile = utils.get_profile(session, arg_user_id, arg_screen_name)
        user_id = profile["id"]
        utils.save_users(db, [profile])
        args = {("user_id" if ids else "screen_name"): identifier}
        for id_batch in utils.cursor_paginate(
            session, api_url, args, "ids", 5000, sleep
        ):
            first_seen = datetime.datetime.utcnow().isoformat()
            db["following"].insert_all(
                (
                    {first_key: user_id, second_key: other_id, "first_seen": first_seen}
                    for other_id in id_batch
                ),
                ignore=True,
            )
        time.sleep(sleep)
