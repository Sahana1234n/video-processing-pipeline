from temporalio import activity
import asyncio
from src.database.clickhouse import insert_metadata
from src.database.lancedb import get_all_processed_frames
from src.utils.failures import maybe_fail

def _store_results_sync(video_id: str) -> None:
    frames = get_all_processed_frames(video_id)

    for frame_id, frame_path in enumerate(frames, start=1):
        maybe_fail()

        metadata = {
            "video_id": video_id,
            "video_name": frame_path,
            "frame_count": frame_id,
        }

        insert_metadata(metadata)

@activity.defn
async def store_results(video_id: str) -> None:
    await asyncio.to_thread(_store_results_sync, video_id)
