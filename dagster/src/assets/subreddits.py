import os

import pandas as pd
from src.resources.db import SQLiteResource
from src.resources.reddit import RedditResource

from dagster import AssetExecutionContext, Config, asset

from .constants import POSTS_LIMIT, SUBREDDIT_FILE_PATH


class SubredditConfig(Config):
    subreddit_list: list[str]


@asset
def subreddit(
    context: AssetExecutionContext,
    config: SubredditConfig,
    reddit: RedditResource,
    sqlite: SQLiteResource,
) -> None:
    subreddits = []
    for subreddit in config.subreddit_list:
        context.log.info("Fetching subreddit %s", subreddit)
        subreddit_df = reddit.get_subreddit(subreddit)
        subreddits.append(subreddit_df)

    subreddit_df = pd.concat(subreddits, ignore_index=True)
    with sqlite.connect() as conn:
        sql_create_table = """
        CREATE TABLE IF NOT EXISTS subreddits (
            id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            url TEXT NOT NULL,
            description TEXT,
            public_description TEXT,
            is_over_18 BOOLEAN NOT NULL,
            subscribers INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );
        """
        conn.execute(sql_create_table)
        subreddit_df.to_sql("subreddits", conn, if_exists="replace", index=False)

    os.makedirs(os.path.dirname(SUBREDDIT_FILE_PATH), exist_ok=True)
    subreddit_df.to_csv(SUBREDDIT_FILE_PATH, index=False, header=True)
    context.log.info("Fetched subreddits")


@asset(
    deps={"subreddit": subreddit},
)
def subreddit_posts(
    context: AssetExecutionContext, reddit: RedditResource, sqlite: SQLiteResource
) -> None:
    context.log.info("Fetching posts from adhoc subreddits request")
    subreddits = pd.read_csv(SUBREDDIT_FILE_PATH)
    subreddit_posts = []
    for subreddit_name in subreddits["display_name"]:
        context.log.info("Fetching posts from %s", subreddit_name)
        subreddit_posts.append(
            reddit.get_subreddit_posts_of_where(subreddit_name, "hot", POSTS_LIMIT)
        )

    subreddit_posts_df = pd.concat(subreddit_posts, ignore_index=True)

    with sqlite.connect() as conn:
        sql_create_table = """
        CREATE TABLE IF NOT EXISTS subreddit_posts (
            id TEXT PRIMARY KEY,
            subreddit_name TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT,
            author TEXT NOT NULL,
            author_id TEXT NOT NULL,
            url TEXT NOT NULL,
            score INTEGER NOT NULL,
            upvote_ratio REAL NOT NULL,
            num_comments INTEGER NOT NULL,
            over_18 BOOLEAN NOT NULL,
            created_at INTEGER NOT NULL
        );
        """
        conn.execute(sql_create_table)
        subreddit_posts_df.to_sql(
            "subreddit_posts", conn, if_exists="replace", index=False
        )

    context.log.info("Fetched posts from adhoc subreddits request")
