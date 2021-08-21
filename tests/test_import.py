import io
import pathlib

import pytest
import sqlite_utils
from click.testing import CliRunner
from twitter_to_sqlite import cli

from .utils import create_zip


@pytest.fixture
def zip_contents_path():
    return pathlib.Path(__file__).parent / "zip_contents"


@pytest.fixture
def import_test_zip(tmpdir, zip_contents_path):
    archive = str(tmpdir / "archive.zip")
    buf = io.BytesIO()
    zf = create_zip(zip_contents_path, buf)
    zf.close()
    open(archive, "wb").write(buf.getbuffer())
    return tmpdir, archive


def test_create_zip(zip_contents_path):
    zf = create_zip(zip_contents_path)
    assert {
        "account-suspension.js",
        "account.js",
        "app.js",
        "saved-search.js",
        "following.js",
        "follower.js",
    } == {f.filename for f in zf.filelist}


def test_cli_import_zip_file(import_test_zip):
    tmpdir, archive = import_test_zip
    output = str(tmpdir / "output.db")
    result = CliRunner().invoke(cli.cli, ["import", output, archive])
    assert 0 == result.exit_code, result.stdout
    db = sqlite_utils.Database(output)
    assert_imported_db(db)


def test_cli_import_folder(tmpdir, zip_contents_path):
    output = str(tmpdir / "output.db")
    result = CliRunner().invoke(cli.cli, ["import", output, str(zip_contents_path)])
    assert 0 == result.exit_code, result.stdout
    db = sqlite_utils.Database(output)
    assert_imported_db(db)


def test_cli_import_specific_files(tmpdir, zip_contents_path):
    output = str(tmpdir / "output.db")
    result = CliRunner().invoke(
        cli.cli,
        [
            "import",
            output,
            str(zip_contents_path / "follower.js"),
            str(zip_contents_path / "following.js"),
        ],
    )
    assert 0 == result.exit_code, result.stdout
    db = sqlite_utils.Database(output)
    # Should just have two tables
    assert ["archive_follower", "archive_following"] == db.table_names()


def assert_imported_db(db):
    assert {
        "archive_follower",
        "archive_saved_search",
        "archive_account",
        "archive_app",
        "archive_following",
    } == set(db.table_names())

    assert [{"accountId": "73747798"}, {"accountId": "386025404"}] == list(
        db["archive_follower"].rows
    )
    assert [{"accountId": "547842573"}, {"accountId": "12158"}] == list(
        db["archive_following"].rows
    )

    assert [{"appId": "1380676511", "appNames": '["BBC Sounds"]'}] == list(
        db["archive_app"].rows
    )

    assert [
        {"savedSearchId": "42214", "query": "simonw"},
        {"savedSearchId": "55814", "query": "django"},
    ] == list(db["archive_saved_search"].rows)
    assert [
        {
            "pk": "c4e32e91742df2331ef3ad1e481d1a64d781183a",
            "phoneNumber": "+15555555555",
            "email": "swillison@example.com",
            "createdVia": "web",
            "username": "simonw",
            "accountId": "12497",
            "createdAt": "2006-11-15T13:18:50.000Z",
            "accountDisplayName": "Simon Willison",
        }
    ] == list(db["archive_account"].rows)


def test_deletes_existing_archive_tables(import_test_zip):
    tmpdir, archive = import_test_zip
    output = str(tmpdir / "output.db")
    db = sqlite_utils.Database(output)
    # Create a table
    db["archive_follower"].create({"id": int})
    db["archive_follower"].insert({"id": 1})
    assert ["archive_follower"] == db.table_names()
    assert [{"id": 1}] == list(db["archive_follower"].rows)
    assert (
        "CREATE TABLE [archive_follower] (\n   [id] INTEGER\n)"
        == db["archive_follower"].schema
    )
    # Running the import should wipe and recreate that table
    CliRunner().invoke(cli.cli, ["import", output, archive])
    # That table should have been deleted and recreated
    assert (
        "CREATE TABLE [archive_follower] (\n   [accountId] TEXT PRIMARY KEY\n)"
        == db["archive_follower"].schema
    )
    assert 2 == db["archive_follower"].count
