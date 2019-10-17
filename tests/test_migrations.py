import sqlite_utils
from click.testing import CliRunner
import sqlite_utils
from twitter_to_sqlite import cli, migrations

from .test_import import zip_contents_path
from .test_save_tweets import db, tweets


def test_no_migrations_on_first_run(tmpdir, zip_contents_path):
    output = str(tmpdir / "output.db")
    args = ["import", output, str(zip_contents_path / "follower.js")]
    result = CliRunner().invoke(cli.cli, args)
    assert 0 == result.exit_code, result.stdout
    db = sqlite_utils.Database(output)
    assert ["archive_follower"] == db.table_names()
    # Re-running the command again should also run the migrations
    result = CliRunner().invoke(cli.cli, args)
    db = sqlite_utils.Database(output)
    assert {"archive_follower", "migrations"} == set(db.table_names())


def test_convert_source_column():
    db = sqlite_utils.Database(memory=True)
    db["tweets"].insert_all(
        [
            {"id": 1, "source": '<a href="URL">NAME</a>'},
            {"id": 2, "source": '<a href="URL2">NAME2</a>'},
            {"id": 3, "source": "d3c1d39c57fecfc09202f20ea5e2db30262029fd"},
        ],
        pk="id",
    )
    migrations.convert_source_column(db)
    assert [
        {
            "id": "d3c1d39c57fecfc09202f20ea5e2db30262029fd",
            "url": "URL",
            "name": "NAME",
        },
        {
            "id": "000e4c4db71278018fb8c322f070d051e76885b1",
            "url": "URL2",
            "name": "NAME2",
        },
    ] == list(db["sources"].rows)
    assert [
        {"id": 1, "source": "d3c1d39c57fecfc09202f20ea5e2db30262029fd"},
        {"id": 2, "source": "000e4c4db71278018fb8c322f070d051e76885b1"},
        {"id": 3, "source": "d3c1d39c57fecfc09202f20ea5e2db30262029fd"},
    ] == list(db["tweets"].rows)


def test_convert_source_column_against_real_database(db):
    assert "migrations" not in db.table_names()
    migrations.convert_source_column(db)
