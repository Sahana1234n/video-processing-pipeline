from typing import List
import lancedb
import pyarrow as pa # type: ignore

DB_PATH = "./lancedb"
TABLE_NAME = "frames"

_db = lancedb.connect(DB_PATH) # type: ignore[attr-defined]

# Define the correct schema
schema = pa.schema([
    pa.field("frame_id", pa.string()),
    pa.field("embedding", pa.list_(pa.float32(), 512)), 
])

if TABLE_NAME in _db.table_names():
    _table = _db.open_table(TABLE_NAME)
else:
    _table = _db.create_table(
        TABLE_NAME,
        schema=schema
    )


def insert_embedding(frame_id: str, embedding: List[float]) -> None:
    if len(embedding) != 512:
        raise ValueError("Embedding must be 512 dimensional")

    _table.add([{
        "frame_id": frame_id,
        "embedding": embedding
    }])

def check_embedding_exists(frame_id: str) -> bool:
    """Return True if this frame_id already exists in the DB."""
    # Convert the 'frame_id' column to a Python list
    frame_ids = _table.to_arrow()["frame_id"].to_pylist()
    return frame_id in frame_ids