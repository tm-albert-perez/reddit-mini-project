from src.assets import subreddits

from dagster import define_asset_job, load_assets_from_modules

subreddits_asset = load_assets_from_modules(modules=[subreddits])

adhoc_subreddit_request_job = define_asset_job(
    name="subreddit_job", selection=subreddits_asset
)
