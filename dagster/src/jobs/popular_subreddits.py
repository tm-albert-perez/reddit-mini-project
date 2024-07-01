from src.assets import popular_subreddits

from dagster import define_asset_job, load_assets_from_modules

subreddits_asset = load_assets_from_modules(modules=[popular_subreddits])

popular_subreddits_job = define_asset_job(
    name="popular_subreddits_job",
    selection=subreddits_asset,
)
