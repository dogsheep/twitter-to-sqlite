import io
import zipfile


def create_zip(path, buf=None):
    if buf is None:
        buf = io.BytesIO()
    zf = zipfile.ZipFile(buf, "w")
    for filepath in path.glob("**/*"):
        if filepath.is_file():
            zf.write(filepath, str(filepath.relative_to(path)))
    return zf
