import os
from temporalio import activity
from temporalio.exceptions import ApplicationError

def maybe_fail() -> None:
    if os.getenv("ENABLE_FAILURE_INJECTION") != "true":
        return

    info = activity.info()

    if info.attempt <= 2:
        raise ApplicationError(
            "Injected failure for resilience testing",
            type="InjectedFailure",
        )
