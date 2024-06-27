from src.assets import popular_subreddits, subreddits
from src.jobs.popular_subreddits import popular_subreddits_job
from src.resources.db import SQLiteResource
from src.resources.reddit import RedditResource
from src.schedules.daily_popular_subreddits import daily_popular_subreddits_schedule
from src.schedules.daily_subreddits import daily_subreddits_schedule
from src.sensors.subreddits import adhoc_request_sensor

from dagster import Definitions, EnvVar, load_assets_from_modules

# Asset definitions
subreddits_asset = load_assets_from_modules(modules=[subreddits])
popular_subreddits_asset = load_assets_from_modules(modules=[popular_subreddits])

# Job definitions
all_jobs = [popular_subreddits_job]

# Schedule definitions
all_schedules = [daily_popular_subreddits_schedule, daily_subreddits_schedule]

# Sensor definitions
all_sensors = [adhoc_request_sensor]

# Resource definitions
reddit_resource = RedditResource(
    client_id=EnvVar("REDDIT_CLIENT_ID"),
    client_secret=EnvVar("REDDIT_CLIENT_SECRET"),
    user_agent=EnvVar("REDDIT_USER_AGENT"),
)
sqlite_resource = SQLiteResource(
    db_path=EnvVar("SQLITE_DB_PATH"),
)

# Definitions
defs = Definitions(
    assets=[*subreddits_asset, *popular_subreddits_asset],
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
    resources={
        "reddit": reddit_resource,
        "sqlite": sqlite_resource,
    },
)
