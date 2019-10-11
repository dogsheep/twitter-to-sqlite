import io
import pathlib
import zipfile


def create_zip(buf=None):
    if buf is None:
        buf = io.BytesIO()
    path = pathlib.Path(__file__).parent / "zip_contents"
    zf = zipfile.ZipFile(buf, "w")
    for filepath in path.glob("**/*"):
        if filepath.is_file():
            zf.write(filepath, str(filepath.relative_to(path)))
    return zf
