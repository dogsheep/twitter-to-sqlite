import pathlib

from .utils import create_zip


def test_create_zip():
    zf = create_zip()
    assert {"account.js", "saved-search.js", "following.js", "follower.js"} == {
        f.filename for f in zf.filelist
    }
