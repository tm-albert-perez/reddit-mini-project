from dagster import define_asset_job

adhoc_subreddit_request_job = define_asset_job(
    name="subreddit_job", selection=["subreddit", "subreddit_posts"]
)
