from temporalio import activity
from temporalio.exceptions import ApplicationError

def maybe_fail() -> None:
    info = activity.info()

    # Fail only first 2 attempts of THIS activity
    if info.attempt <= 2:
        raise ApplicationError(
            "Injected failure for resilience testing",
            type="InjectedFailure",
            non_retryable=False,
        )
