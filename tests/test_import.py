import io

import pytest
import sqlite_utils
from click.testing import CliRunner
from twitter_to_sqlite import cli

from .utils import create_zip


@pytest.fixture
def import_test_dir(tmpdir):
    archive = str(tmpdir / "archive.zip")
    buf = io.BytesIO()
    zf = create_zip(buf)
    zf.close()
    open(archive, "wb").write(buf.getbuffer())
    return tmpdir, archive


def test_cli_import(import_test_dir):
    tmpdir, archive = import_test_dir
    output = str(tmpdir / "output.db")
    result = CliRunner().invoke(cli.cli, ["import", output, archive])
    assert 0 == result.exit_code, result.stderr
    db = sqlite_utils.Database(output)
    assert {
        "archive_follower",
        "archive_saved_search",
        "archive_account",
        "archive_following",
    } == set(db.table_names())

    assert [{"accountId": "73747798"}, {"accountId": "386025404"}] == list(
        db["archive_follower"].rows
    )
    assert [{"accountId": "547842573"}, {"accountId": "12158"}] == list(
        db["archive_following"].rows
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


def test_deletes_existing_archive_tables(import_test_dir):
    tmpdir, archive = import_test_dir
    output = str(tmpdir / "output.db")
    db = sqlite_utils.Database(output)
    # Create a table
    db["archive_foo"].create({"id": int})
    assert ["archive_foo"] == db.table_names()
    result = CliRunner().invoke(cli.cli, ["import", output, archive])
    # That table should have been deleted
    assert "archive_foo" not in db.table_names()
