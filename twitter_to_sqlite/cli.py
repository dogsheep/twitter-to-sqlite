import datetime
import hashlib
import json
import os
import pathlib
import time

import click

from twitter_to_sqlite import archive
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
@add_identifier_options
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True, exists=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option("--ids", is_flag=True, help="Treat input as user IDs, not screen names")
@click.option("--silent", is_flag=True, help="Disable progress bar")
def followers(db_path, identifiers, attach, sql, auth, ids, silent):
    "Save followers for specified users (defaults to authenticated user)"
    _shared_friends_followers(
        db_path, identifiers, attach, sql, auth, ids, silent, "followers"
    )


def _shared_friends_followers(
    db_path, identifiers, attach, sql, auth, ids, silent, noun
):
    assert noun in ("friends", "followers")
    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    db = utils.open_database(db_path)

    identifiers = utils.resolve_identifiers(db, identifiers, attach, sql)

    if not identifiers:
        profile = utils.get_profile(db, session)
        identifiers = [profile["screen_name"]]

    for identifier in identifiers:
        if ids:
            kwargs = {"user_id": identifier}
        else:
            kwargs = {"screen_name": identifier}

        fetched = []
        # Get the follower count, so we can have a progress bar
        count = 0

        profile = utils.get_profile(db, session, **kwargs)
        screen_name = profile["screen_name"]
        user_id = profile["id"]

        save_users_kwargs = {}
        if noun == "followers":
            save_users_kwargs["followed_id"] = user_id
        elif noun == "friends":
            save_users_kwargs["follower_id"] = user_id

        def go(update):
            for users_chunk in utils.fetch_user_list_chunks(
                session, user_id, screen_name, noun=noun
            ):
                fetched.extend(users_chunk)
                utils.save_users(db, users_chunk, **save_users_kwargs)
                update(len(users_chunk))

        if not silent:
            count = profile["{}_count".format(noun)]
            with click.progressbar(
                length=count,
                label="Importing {:,} {} for @{}".format(count, noun, screen_name),
            ) as bar:
                go(bar.update)
        else:
            go(lambda x: None)


@cli.command()
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
@click.option("--silent", is_flag=True, help="Disable progress bar")
def friends(db_path, identifiers, attach, sql, auth, ids, silent):
    "Save friends for specified users (defaults to authenticated user)"
    _shared_friends_followers(
        db_path, identifiers, attach, sql, auth, ids, silent, "friends"
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
@click.option("--stop_after", type=int, help="Stop after this many")
def favorites(db_path, auth, user_id, screen_name, stop_after):
    "Save tweets favorited by specified user"
    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    db = utils.open_database(db_path)
    profile = utils.get_profile(db, session, user_id, screen_name)
    with click.progressbar(
        utils.fetch_favorites(session, user_id, screen_name, stop_after),
        label="Importing favorites",
        show_pos=True,
    ) as bar:
        utils.save_tweets(db, bar, favorited_by=profile["id"])


@cli.command(name="user-timeline")
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
@click.option("--stop_after", type=int, help="Only pull this number of recent tweets")
@click.option("--user_id", help="Numeric user ID", hidden=True)
@click.option("--screen_name", help="Screen name", hidden=True)
@click.option(
    "--since",
    is_flag=True,
    default=False,
    help="Pull tweets since last retrieved tweet",
)
@click.option(
    "--since_id", type=str, default=False, help="Pull tweets since this Tweet ID"
)
def user_timeline(
    db_path,
    identifiers,
    attach,
    sql,
    auth,
    ids,
    stop_after,
    user_id,
    screen_name,
    since,
    since_id,
):
    "Save tweets posted by specified user"
    if since and since_id:
        raise click.ClickException("Use either --since or --since_id, not both")

    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    db = utils.open_database(db_path)
    identifiers = utils.resolve_identifiers(db, identifiers, attach, sql)

    # Backwards compatible support for old --user_id and --screen_name options
    if screen_name:
        if ids:
            raise click.ClickException("Cannot use --screen_name with --ids")
        identifiers.append(screen_name)

    if user_id:
        if not identifiers:
            identifiers = [user_id]
        else:
            if not ids:
                raise click.ClickException("Use --user_id with --ids")
            identifiers.append(user_id)

    # If identifiers is empty, fetch the authenticated user
    fetch_profiles = True
    if not identifiers:
        fetch_profiles = False
        profile = utils.get_profile(db, session, user_id, screen_name)
        identifiers = [profile["screen_name"]]
        ids = False

    format_string = (
        "@{:" + str(max(len(identifier) for identifier in identifiers)) + "}"
    )

    for identifier in identifiers:
        kwargs = {}
        if ids:
            kwargs["user_id"] = identifier
        else:
            kwargs["screen_name"] = identifier
        if fetch_profiles:
            profile = utils.get_profile(db, session, **kwargs)
        else:
            profile = db["users"].get(profile["id"])
        expected_length = profile["statuses_count"]

        if since or since_id:
            expected_length = None

        if since and db["tweets"].exists:
            try:
                since_id = db.conn.execute(
                    "select max(id) from tweets where user = ?", [profile["id"]]
                ).fetchall()[0][0]
            except IndexError:
                pass

        with click.progressbar(
            utils.fetch_user_timeline(
                session, stop_after=stop_after, since_id=since_id, **kwargs
            ),
            length=expected_length,
            label=format_string.format(profile["screen_name"]),
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


@cli.command(name="home-timeline")
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
@click.option(
    "--since",
    is_flag=True,
    default=False,
    help="Pull tweets since last retrieved tweet",
)
@click.option(
    "--since_id", type=str, default=False, help="Pull tweets since this Tweet ID"
)
def home_timeline(db_path, auth, since, since_id):
    "Save tweets from timeline for authenticated user"
    _shared_timeline(
        db_path,
        auth,
        since,
        since_id,
        table="timeline_tweets",
        api_url="https://api.twitter.com/1.1/statuses/home_timeline.json",
    )


@cli.command(name="mentions-timeline")
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
@click.option(
    "--since",
    is_flag=True,
    default=False,
    help="Pull tweets since last retrieved mention",
)
@click.option(
    "--since_id", type=str, default=False, help="Pull mentions since this Tweet ID"
)
def mentions_timeline(db_path, auth, since, since_id):
    "Save tweets that mention the authenticated user"
    _shared_timeline(
        db_path,
        auth,
        since,
        since_id,
        table="mentions_tweets",
        api_url="https://api.twitter.com/1.1/statuses/mentions_timeline.json",
        sleep=10,
    )


def _shared_timeline(db_path, auth, since, since_id, table, api_url, sleep=1):
    if since and since_id:
        raise click.ClickException("Use either --since or --since_id, not both")
    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    db = utils.open_database(db_path)
    profile = utils.get_profile(db, session)
    expected_length = 800
    if since and db[table].exists:
        # Set since_id to highest value for this timeline
        try:
            since_id = db.conn.execute(
                "select max(tweet) from {} where user = ?".format(table),
                [profile["id"]],
            ).fetchall()[0][0]
            expected_length = None
        except IndexError:
            pass

    with click.progressbar(
        utils.fetch_timeline(session, api_url, sleep=sleep, since_id=since_id),
        length=expected_length,
        label="Importing tweets",
        show_pos=True,
    ) as bar:
        # Save them 100 at a time
        def save_chunk(db, chunk):
            utils.save_tweets(db, chunk)
            # Record who's timeline they came from
            db[table].insert_all(
                [{"user": profile["id"], "tweet": tweet["id"]} for tweet in chunk],
                pk=("user", "tweet"),
                foreign_keys=("user", "tweet"),
                replace=True,
            )

        chunk = []
        for tweet in bar:
            chunk.append(tweet)
            if len(chunk) >= 100:
                save_chunk(db, chunk)
                chunk = []
        if chunk:
            save_chunk(db, chunk)


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
    db = utils.open_database(db_path)
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
    db = utils.open_database(db_path)
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
    db = utils.open_database(db_path)
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
    db = utils.open_database(db_path)
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
    db = utils.open_database(db_path)
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
    db = utils.open_database(db_path)
    identifiers = utils.resolve_identifiers(db, identifiers, attach, sql)
    for identifier in identifiers:
        # Make sure this user is saved
        arg_user_id = identifier if ids else None
        arg_screen_name = None if ids else identifier
        profile = utils.get_profile(db, session, arg_user_id, arg_screen_name)
        user_id = profile["id"]
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


@cli.command(name="import")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=True, allow_dash=False),
    required=True,
)
@click.argument(
    "paths",
    type=click.Path(file_okay=True, dir_okay=True, allow_dash=False, exists=True),
    required=True,
    nargs=-1,
)
def import_(db_path, paths):
    """
    Import data from a Twitter exported archive. Input can be the path to a zip
    file, a directory full of .js files or one or more direct .js files.
    """
    db = utils.open_database(db_path)
    for filepath in paths:
        path = pathlib.Path(filepath)
        if path.suffix == ".zip":
            for filename, content in utils.read_archive_js(filepath):
                archive.import_from_file(db, filename, content)
        elif path.is_dir():
            # Import every .js file in this directory
            for filepath in path.glob("*.js"):
                archive.import_from_file(db, filepath.name, open(filepath, "rb").read())
        elif path.suffix == ".js":
            archive.import_from_file(db, path.name, open(path, "rb").read())
        else:
            raise click.ClickException("Path must be a .js or .zip file or a directory")


@cli.command()
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("q")
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True, exists=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option(
    "--since",
    is_flag=True,
    default=False,
    help="Pull tweets since last retrieved tweet",
)
@click.option(
    "--geocode",
    type=str,
    help="latitude,longitude,radius - where radius is a number followed by mi or km",
)
@click.option("--lang", type=str, help="ISO 639-1 language code")
@click.option("--locale", type=str, help="Locale: only 'ja' is currently effective")
@click.option("--result_type", type=click.Choice(["mixed", "recent", "popular"]))
@click.option("--count", type=int, default=100, help="Number of results per page")
@click.option("--stop_after", type=int, help="Stop after this many")
@click.option(
    "--since_id", type=str, default=False, help="Pull tweets since this Tweet ID"
)
def search(db_path, q, auth, since, **kwargs):
    """
    Save tweets from a search. Full documentation here:

    https://developer.twitter.com/en/docs/tweets/search/api-reference/get-search-tweets
    """
    since_id = kwargs.pop("since_id", None)
    if since and since_id:
        raise click.ClickException("Use either --since or --since_id, not both")
    stop_after = kwargs.pop("stop_after", None)
    auth = json.load(open(auth))
    session = utils.session_for_auth(auth)
    db = utils.open_database(db_path)

    search_args = {"q": q}
    for key, value in kwargs.items():
        if value is not None:
            search_args[key] = value

    args_hash = hashlib.sha1(
        json.dumps(search_args, sort_keys=True, separators=(",", ":")).encode("utf8")
    ).hexdigest()

    if since and db["search_runs_tweets"].exists:
        # Find the maximum tweet ID from previous runs of this search
        try:
            since_id = db.conn.execute(
                """
                select max(tweet) from search_runs_tweets where search_run in (
                    select id from search_runs where hash = ?
                )
                """,
                [args_hash],
            ).fetchall()[0][0]
        except IndexError:
            pass

    tweets = utils.fetch_timeline(
        session,
        "https://api.twitter.com/1.1/search/tweets.json",
        search_args,
        sleep=6,
        key="statuses",
        stop_after=stop_after,
        since_id=since_id,
    )
    chunk = []
    first = True

    if not db["search_runs"].exists:
        db["search_runs"].create(
            {"id": int, "name": str, "args": str, "started": str, "hash": str}, pk="id"
        )

    def save_chunk(db, search_run_id, chunk):
        utils.save_tweets(db, chunk)
        # Record which search run produced them
        db["search_runs_tweets"].insert_all(
            [{"search_run": search_run_id, "tweet": tweet["id"]} for tweet in chunk],
            pk=("search_run", "tweet"),
            foreign_keys=("search_run", "tweet"),
            replace=True,
        )

    search_run_id = None
    for tweet in tweets:
        if first:
            first = False
            search_run_id = (
                db["search_runs"]
                .insert(
                    {
                        "name": search_args["q"],
                        "args": {
                            key: value
                            for key, value in search_args.items()
                            if key not in {"q", "count"}
                        },
                        "started": datetime.datetime.utcnow().isoformat(),
                        "hash": args_hash,
                    },
                    alter=True,
                )
                .last_pk
            )
        chunk.append(tweet)
        if len(chunk) >= 10:
            save_chunk(db, search_run_id, chunk)
            chunk = []
    if chunk:
        save_chunk(db, search_run_id, chunk)
