from typing import List
import lancedb
import pyarrow as pa  # type: ignore[attr-defined]
import shutil
import os

DB_PATH = "./lancedb"
TABLE_NAME = "frames"
TABLE_PATH = os.path.join(DB_PATH, f"{TABLE_NAME}.lance")

# Define the correct schema
schema = pa.schema([
    pa.field("video_id", pa.string()),
    pa.field("frame_path", pa.string()),
    pa.field("embedding", pa.list_(pa.float32(), 512)),
])

# Ensure DB folder exists
os.makedirs(DB_PATH, exist_ok=True)

# Drop old table if schema might be incorrect
if os.path.exists(TABLE_PATH):
    shutil.rmtree(TABLE_PATH)
    print(f"Deleted old table at {TABLE_PATH}")

# Connect to DB and create table
_db = lancedb.connect(DB_PATH)  # type: ignore[attr-defined]
_table = _db.create_table(TABLE_NAME, schema=schema)
print(f"Created table '{TABLE_NAME}' with correct schema.")

# Core functions
def insert_embedding(video_id: str, frame_path: str, embedding: List[float]) -> None:
    """
    Insert a 512-dimensional embedding into LanceDB
    """
    if len(embedding) != 512:
        raise ValueError("Embedding must be 512-dimensional")

    _table.add([{
        "video_id": video_id,
        "frame_path": frame_path,
        "embedding": embedding,
    }])

def check_embedding_exists(video_id: str, frame_path: str) -> bool:
    """
    Check if an embedding already exists in the table
    """
    df = _table.to_pandas()
    filtered = df[(df["video_id"] == video_id) & (df["frame_path"] == frame_path)]
    return not filtered.empty

def get_all_processed_frames(video_id: str) -> List[str]:
    """
    Get all processed frames for a given video_id
    """
    df = _table.to_pandas()
    filtered = df[df["video_id"] == video_id]
    return filtered["frame_path"].tolist()
