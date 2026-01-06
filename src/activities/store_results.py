from typing import List
from temporalio import activity
import asyncio
import logging
from src.database.clickhouse import insert_metadata
from src.utils.failures import maybe_fail

# synchronous logic
def _store_results_sync(video_id: str, frames: List[str]) -> None:
    for frame_id, frame_path in enumerate(frames):

        # Failure injection for resilience testing
        maybe_fail()

        # Prepare metadata dictionary
        metadata = {
            "video_id": str(video_id),
            "video_name": str(frame_path),
            "frame_count": int(frame_id + 1),
            "processed_at": None
        }
        insert_metadata(metadata)
                
# async wrapper decorated as a Temporal activity
@activity.defn
async def store_results(video_id: str, frames: List[str]) -> None:
    await asyncio.to_thread(_store_results_sync, video_id, frames)
