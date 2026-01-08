from typing import List
import cv2
from pathlib import Path
import logging
from temporalio import activity
import asyncio

logging.basicConfig(level=logging.INFO)

from src.utils.failures import maybe_fail

# synchronous function
def _extract_frames_sync(
    video_path: str,
    output_dir: str = "/tmp/frames",
    frame_step: int = 5,
    max_frames: int = 500
) -> List[str]:
    """
    Extract frames from a video file and save them as images
    """

    # Failure injection
    maybe_fail()

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise ValueError(f"Unable to open video file: {video_path}")

    frames: List[str] = []
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    video_name = Path(video_path).stem
    frame_id = 0
    saved_count = 0

    try:
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break

            # Skip frames for performance
            if frame_id % frame_step != 0:
                frame_id += 1
                continue

            if saved_count >= max_frames:
                logging.info("Max frame limit reached")
                break

            frame_path = Path(output_dir) / f"{video_name}_frame_{saved_count}.jpg"
            success = cv2.imwrite(str(frame_path), frame)

            if not success:
                raise RuntimeError(f"Failed to write frame {saved_count}")

            frames.append(str(frame_path))
            logging.info(f"Saved frame: {frame_path}")

            frame_id += 1
            saved_count += 1

    finally:
        cap.release()

    return frames


# Async wrapper for Temporal
@activity.defn
async def extract_frames(
    video_path: str,
    output_dir: str = "/tmp/frames",
    frame_step: int = 5,
    max_frames: int = 500
) -> List[str]:
    """
    Async wrapper around the synchronous frame extraction.
    """
    return await asyncio.to_thread(
        _extract_frames_sync, video_path, output_dir, frame_step, max_frames
    )
