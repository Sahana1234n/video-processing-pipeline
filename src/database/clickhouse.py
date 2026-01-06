import os
import logging
import clickhouse_connect
from dotenv import load_dotenv
from typing import Dict

# Load environment variables from .env
load_dotenv()

logging.basicConfig(level=logging.INFO)

# ClickHouse credentials from environment
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8443"))
CLICKHOUSE_USERNAME = os.getenv("CLICKHOUSE_USERNAME")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
if CLICKHOUSE_PASSWORD is None:
    raise RuntimeError("CLICKHOUSE_PASSWORD not set")

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

# Check if the table "video_metadata" exists, create if it doesn't
tables =  [row[0] for row in client.query("SHOW TABLES").result_rows]
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
    ORDER BY video_id
    """)
    logging.info("Table 'video_metadata' created successfully.")

# Function to insert metadata
def insert_metadata(metadata: Dict) -> None:
    """
    Insert metadata into ClickHouse table "video_metadata".
    """
    table_name = "video_metadata"

    #only allowed columns that exists in table
    allowed_columns = {"video_id", "video_name", "frame_count", "processed_at"}

    #Always include all columns and use defaults if missing
    clean_metadata = {col: metadata.get(col, None) for col in allowed_columns}

    # Check required fields
    if clean_metadata["video_id"] is None:
        raise ValueError("video_id is required in metadata")
    
    client.insert(table_name, [clean_metadata]) #type: ignore
    logging.info(f"Inserted metadata for video_id: {metadata.get('video_id')}")
