from typing import List
import numpy as np
from temporalio import activity
import logging
from src.utils.failures import maybe_fail
from src.database.lancedb import insert_embedding, check_embedding_exists
import asyncio 

BATCH_SIZE = 50 

@activity.defn
async def process_frames(video_id: str, frames: List[str]) -> List[str]:
    """
    Processes frames in batches with heartbeat-safe execution.
    """
    processed_frames: List[str] = []
    total = len(frames)

    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch_frames = frames[batch_start:batch_end]

        logging.info(f"Processing batch {batch_start+1}-{batch_end} / {total}")

        for idx, frame_path in enumerate(batch_frames, start=batch_start + 1):
            try:
                # Heartbeat per frame
                activity.heartbeat(f"Starting frame {idx}/{total}")

                # Idempotency check
                if check_embedding_exists(video_id, frame_path):
                    logging.info(f"Skipping already processed frame {idx}/{total}: {frame_path}")
                    processed_frames.append(frame_path)
                    continue

                logging.info(f"Processing frame {idx}/{total}: {frame_path}")

                # Failure injection 
                maybe_fail()

                # Generate embedding in chunks
                embedding = []
                for i in range(8):  
                    chunk = np.random.rand(64).tolist()
                    embedding.extend(chunk)

                    # Heartbeat every 2 chunks
                    if i % 2 == 0:
                        activity.heartbeat(f"Processing frame {idx}/{total}, embedding {i*64+1}/512")
                    await asyncio.sleep(0.005)

                maybe_fail()

                # Insert embedding
                insert_embedding(video_id, frame_path, embedding)
                processed_frames.append(frame_path)

                # Heartbeat after finishing frame
                activity.heartbeat(f"Completed frame {idx}/{total}")

            except Exception as e:
                logging.error(f"Failed processing frame {idx}/{total}: {frame_path}, error: {e}")
                raise

    return processed_frames
