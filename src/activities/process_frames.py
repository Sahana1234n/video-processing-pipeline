from typing import List
import numpy as np
from temporalio import activity
import asyncio
import logging
from src.utils.failures import maybe_fail
from src.database.lancedb import insert_embedding
from src.database.lancedb import insert_embedding, check_embedding_exists

# synchronous processing function (idempotent

def _process_frames_sync(frames: List[str]) -> List[str]:
    processed_frames = []

    for i, frame_path in enumerate(frames):
        # Skip if already processed
        if check_embedding_exists(frame_path):
            logging.info(f"Skipping already processed frame {i+1}/{len(frames)}: {frame_path}")
            processed_frames.append(frame_path)
            continue
        logging.info(f"Processing frame {i+1}/{len(frames)}: {frame_path}")
      
        # Simulate ML inference failure
        maybe_fail()

        embedding = np.random.rand(512).tolist()

        # Simulate DB insertion failure
        maybe_fail()

        insert_embedding(frame_path, embedding)

        # Keep track of successfully processed frames
        processed_frames.append(frame_path)

    return processed_frames

# async wrapper decorated as a Temporal activity
@activity.defn
async def process_frames(frames: List[str]) -> List[str]:
    return await asyncio.to_thread(_process_frames_sync, frames)
