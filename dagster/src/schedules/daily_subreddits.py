import json
import os

from src.jobs.subreddits import adhoc_subreddit_request_job

from dagster import RunRequest, SensorEvaluationContext, SkipReason, schedule


@schedule(
    job=adhoc_subreddit_request_job,
    name="daily_subreddits_schedule",
    cron_schedule="0 23 * * *",  # every day at 11pm
)
def daily_subreddits_schedule(context: SensorEvaluationContext):
    context.log.info("Running daily subreddits schedule")
    PATH_TO_REQUESTS = os.path.join(
        os.path.dirname(__file__), "../../", "data/requests/favorites.json"
    )
    if not os.path.exists(PATH_TO_REQUESTS):
        context.log.info("No requests to process")
        return SkipReason("No requests to process")

    with open(PATH_TO_REQUESTS, "r") as f:
        request_config = json.load(f)
        return RunRequest(
            run_key="subreddit_favorites",
            run_config={"ops": {"subreddit": {"config": {**request_config}}}},
        )
