from dagster import define_asset_job

popular_subreddits_job = define_asset_job(
    name="popular_subreddits_job",
    selection=["popular_subreddits", "popular_subreddits_posts"],
)
