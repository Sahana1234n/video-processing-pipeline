import asyncio
from temporalio.client import Client
from temporalio.worker import Worker

from src.workflows.workflow import VideoWorkflow
from src.activities.extract_frames import extract_frames
from src.activities.process_frames import process_frames
from src.activities.store_results import store_results

async def main() -> None:
    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue="video-task-queue",
        workflows=[VideoWorkflow],
        activities=[
            extract_frames,
            process_frames,
            store_results,
        ],
    )

    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
