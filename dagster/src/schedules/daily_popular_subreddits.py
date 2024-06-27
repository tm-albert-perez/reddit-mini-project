from src.jobs.popular_subreddits import popular_subreddits_job

from dagster import RunRequest, ScheduleEvaluationContext, schedule


@schedule(
    job=popular_subreddits_job,
    name="daily_popular_subreddits_schedule",
    cron_schedule="0 23 * * *",  # every day at 11pm
)
def daily_popular_subreddits_schedule(context: ScheduleEvaluationContext):
    context.log.info("Running daily popular subreddits schedule")
    return RunRequest(run_key="popular_subreddits")
