from temporalio import activity
import asyncio
from src.database.clickhouse import insert_metadata
from src.database.lancedb import get_all_processed_frames
from src.utils.failures import maybe_fail
import logging

logging.basicConfig(level=logging.INFO)


def _store_results_sync(video_id: str) -> None:
    """
    Synchronous helper to fetch processed frames and store metadata.
    Runs in a separate thread from the async Temporal activity.
    """
    logging.info(f"Fetching processed frames for video_id={video_id}")
    frames = get_all_processed_frames(video_id)

    if not frames:
        logging.warning(f"No frames found for video_id={video_id}")
        return

    for frame_id, frame_path in enumerate(frames, start=1):
        # Simulate failure for resilience testing
        maybe_fail()

        metadata = {
            "video_id": video_id,
            "video_name": frame_path,
            "frame_count": frame_id,
        }

        try:
            insert_metadata(metadata)
        except Exception as e:
            logging.error(f"Error storing metadata for frame {frame_id} of video_id={video_id}: {e}")


@activity.defn
async def store_results(video_id: str) -> None:
    """
    Temporal activity to store metadata for all processed frames of a video.
    Runs synchronous code in a thread to avoid blocking the event loop.
    """
    logging.info(f"Starting store_results activity for video_id={video_id}")
    await asyncio.to_thread(_store_results_sync, video_id)
    logging.info(f"Completed store_results activity for video_id={video_id}")
