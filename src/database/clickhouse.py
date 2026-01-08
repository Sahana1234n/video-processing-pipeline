import os
import logging
import clickhouse_connect
from dotenv import load_dotenv
from typing import Dict

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)

# ClickHouse credentials
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8443"))
CLICKHOUSE_USERNAME = os.getenv("CLICKHOUSE_USERNAME")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

if not all([CLICKHOUSE_HOST, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD]):
    raise RuntimeError("ClickHouse credentials not found in environment")

# Create ClickHouse client
client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USERNAME,
    password=CLICKHOUSE_PASSWORD,
    secure=True
)
logging.info("Connected to ClickHouse Cloud")

# Ensure table exists
tables = [row[0] for row in client.query("SHOW TABLES").result_rows]
if "video_metadata" not in tables:
    logging.info("Table 'video_metadata' not found. Creating table...")
    client.command("""
    CREATE TABLE video_metadata (
        video_id String,
        video_name String,
        frame_count UInt32,
        processed_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree()
    ORDER BY (video_id, frame_count)
    """)
    logging.info("Table 'video_metadata' created successfully.")


def insert_metadata(metadata: Dict) -> None:
    """
    Insert metadata into ClickHouse table 'video_metadata'.
    """
    table_name = "video_metadata"

    # Ensure required fields
    video_id = metadata.get("video_id")
    if not video_id:
        raise ValueError("video_id is required in metadata")

    video_name = str(metadata.get("video_name", ""))
    frame_count = int(metadata.get("frame_count", 0))

    try:
        client.insert(
            table_name,
            [(video_id, video_name, frame_count)],
            column_names=["video_id", "video_name", "frame_count"]
        )
        logging.info(f"Inserted metadata for video_id={video_id}, frame_count={frame_count}")
    except Exception as e:
        logging.error(f"Failed to insert metadata for video_id={video_id}, frame_count={frame_count}: {e}")
        raise
