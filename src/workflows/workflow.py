from temporalio import workflow
from temporalio.common import RetryPolicy
from datetime import timedelta

with workflow.unsafe.imports_passed_through():
    from src.activities.extract_frames import extract_frames
    from src.activities.process_frames import process_frames
    from src.activities.store_results import store_results


@workflow.defn
class VideoWorkflow:
    @workflow.run
    async def run(self, video_path: str) -> None:

        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=20),
            maximum_attempts=5,
        )

        # Extract frames
        frames = await workflow.execute_activity(
            extract_frames,
            video_path,
            start_to_close_timeout=timedelta(minutes=10),
            schedule_to_close_timeout=timedelta(minutes=50),
            retry_policy=retry_policy,
        )

        # Process frames
        processed_frames = await workflow.execute_activity(
            process_frames,
            frames,
            start_to_close_timeout=timedelta(minutes=10),
            schedule_to_close_timeout=timedelta(minutes=50),
            retry_policy=retry_policy,
        )

        # Store results
        result = await workflow.execute_activity(
            store_results,
            args = (video_path, processed_frames),
            start_to_close_timeout=timedelta(minutes=10),
            schedule_to_close_timeout=timedelta(minutes=50),
            retry_policy=retry_policy,
        )

        # Nothing to return because -> None

