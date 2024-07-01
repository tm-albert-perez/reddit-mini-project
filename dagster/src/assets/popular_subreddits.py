import os

import pandas as pd
from src.resources.db import SQLiteResource
from src.resources.reddit import RedditResource

from dagster import AssetExecutionContext, asset

from .constants import (
    POPULAR_SUBREDDITS_FILE_PATH,
    POPULAR_SUBREDDITS_LIMIT,
    POPULAR_SUBREDDITS_POSTS_FILE_PATH,
    POSTS_LIMIT,
)


@asset
def popular_subreddits_raw(
    context: AssetExecutionContext, reddit: RedditResource
) -> None:
    context.log.info("Fetching %s most popular subreddits", POPULAR_SUBREDDITS_LIMIT)
    popular_subreddits_df = reddit.get_subreddits_where(
        category="popular", limit=POPULAR_SUBREDDITS_LIMIT
    )

    os.makedirs(os.path.dirname(POPULAR_SUBREDDITS_FILE_PATH), exist_ok=True)
    popular_subreddits_df.to_csv(POPULAR_SUBREDDITS_FILE_PATH, index=False, header=True)
    context.log.info("Fetched %s most popular subreddits", POPULAR_SUBREDDITS_LIMIT)


@asset(
    deps={"popular_subreddits_raw": popular_subreddits_raw},
)
def popular_subreddits(context: AssetExecutionContext, sqlite: SQLiteResource) -> None:
    popular_subreddits_df = pd.read_csv(POPULAR_SUBREDDITS_FILE_PATH)
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
        popular_subreddits_df.to_sql(
            "popular_subreddits", conn, if_exists="replace", index=False
        )

    context.log.info(
        "Saved %s most popular subreddits to SQLite", POPULAR_SUBREDDITS_LIMIT
    )


@asset(
    deps={"popular_subreddits_raw": popular_subreddits_raw},
)
def popular_subreddit_posts_raw(
    context: AssetExecutionContext, reddit: RedditResource
) -> None:
    context.log.info("Fetching posts from popular subreddits")
    popular_subreddits = pd.read_csv(POPULAR_SUBREDDITS_FILE_PATH)
    subreddit_posts = []
    for subreddit_name in popular_subreddits["display_name"]:
        context.log.info("Fetching posts from %s", subreddit_name)
        subreddit_posts.append(
            reddit.get_subreddit_posts_of_where(subreddit_name, "hot", POSTS_LIMIT)
        )

    subreddit_posts_df = pd.concat(subreddit_posts, ignore_index=True)
    subreddit_posts_df.to_csv(
        POPULAR_SUBREDDITS_POSTS_FILE_PATH, index=False, header=True
    )
    context.log.info("Fetched posts from popular subreddits")


@asset(
    deps={"popular_subreddit_posts_raw": popular_subreddit_posts_raw},
)
def popular_subreddits_posts(
    context: AssetExecutionContext, sqlite: SQLiteResource
) -> None:
    subreddit_posts_df = pd.read_csv(POPULAR_SUBREDDITS_POSTS_FILE_PATH)
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
    context.log.info("Saved posts from popular subreddits to SQLite")
