import click
import os
import sqlite_utils
import json
from twitter_to_sqlite import utils


@click.group()
@click.version_option()
def cli():
    "Save data from Twitter to a SQLite database"


@cli.command()
@click.argument(
    "auth_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    default="auth.json",
)
def auth(auth_path):
    "Save authentication credentials to a JSON file"
    click.echo("Create an app here: https://developer.twitter.com/en/apps")
    click.echo("Then navigate to 'Keys and tokens' and paste in the following:")
    click.echo()
    api_key = click.prompt("API key")
    api_secret_key = click.prompt("API secret key")
    access_token = click.prompt("Access token")
    access_token_secret = click.prompt("Access token secret")
    open(auth_path, "w").write(
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
@click.argument(
    "auth_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True, exists=True),
    default="auth.json",
)
@click.option("--user_id", help="Numeric user ID")
@click.option("--screen_name", help="Screen name")
@click.option("--silent", is_flag=True, help="Disable progress bar")
def followers(db_path, auth_path, user_id, screen_name, silent):
    "Save followers for specified user (defaults to authenticated user)"
    auth = json.load(open(auth_path))
    session = utils.session_for_auth(auth)
    db = sqlite_utils.Database(db_path)
    fetched = []
    # Get the follower count, so we can have a progress bar
    count = 0

    def go(update):
        for followers_chunk in utils.fetch_follower_chunks(
            session, user_id, screen_name
        ):
            fetched.extend(followers_chunk)
            with db.conn:
                db["followers"].upsert_all(followers_chunk)
            update(len(followers_chunk))

    if not silent:
        profile = utils.get_profile(session, user_id, screen_name)
        screen_name = profile["screen_name"]
        count = profile["followers_count"]
        with click.progressbar(
            length=count,
            label="Importing {:,} followers for @{}".format(count, screen_name),
        ) as bar:
            go(bar.update)
    else:
        go(lambda x: None)
    open("/tmp/all.json", "w").write(json.dumps(fetched, indent=4))
