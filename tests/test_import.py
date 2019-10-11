import io

import pytest
import sqlite_utils
from click.testing import CliRunner
from twitter_to_sqlite import cli

from .utils import create_zip


def test_cli_import(tmpdir):
    archive = str(tmpdir / "archive.zip")
    output = str(tmpdir / "output.db")
    buf = io.BytesIO()
    zf = create_zip(buf)
    zf.close()
    open(archive, "wb").write(buf.getbuffer())
    result = CliRunner().invoke(cli.cli, ["import", output, archive])
    assert 0 == result.exit_code, result.stderr
    db = sqlite_utils.Database(output)
    assert {
        "archive-follower",
        "archive-saved-search",
        "archive-account",
        "archive-following",
    } == set(db.table_names())

    assert [{"accountId": "73747798"}, {"accountId": "386025404"}] == list(
        db["archive-follower"].rows
    )
    assert [{"accountId": "547842573"}, {"accountId": "12158"}] == list(
        db["archive-following"].rows
    )

    assert [
        {"savedSearchId": "42214", "query": "simonw"},
        {"savedSearchId": "55814", "query": "django"},
    ] == list(db["archive-saved-search"].rows)
    dd = list(db["archive-account"].rows)
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
    ] == dd
