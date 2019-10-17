from .utils import extract_and_save_source

MIGRATIONS = []


def migration(fn):
    MIGRATIONS.append(fn)
    return fn


@migration
def convert_source_column(db):
    tables = set(db.table_names())
    if "tweets" not in tables:
        return
    # Now we extract any '<a href=...' records from the source
    for id, source in db.conn.execute(
        "select id, source from tweets where source like '<%'"
    ).fetchall():
        db["tweets"].update(id, {"source": extract_and_save_source(db, source)})
    try:
        db["tweets"].create_index(["source"])
    except Exception:
        pass
    try:
        db["tweets"].add_foreign_key("source")
    except Exception:
        pass
