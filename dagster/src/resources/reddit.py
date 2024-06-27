import pandas as pd
import praw

from dagster import ConfigurableResource


class RedditResource(ConfigurableResource):
    client_id: str
    client_secret: str
    user_agent: str

    @property
    def client(self):
        return praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent,
        )

    def get_subreddits_where(self, category: str, limit: int) -> pd.DataFrame:
        assert category in ["popular", "new"]

        subreddits_gen = None
        if category == "popular":
            subreddits_gen = self.client.subreddits.popular(limit=limit)
        if category == "new":
            subreddits_gen = self.client.subreddits.new(limit=limit)

        subreddits = []
        for subreddit in subreddits_gen:
            subreddits.append(
                {
                    "display_name": subreddit.display_name,
                    "url": f"https://www.reddit.com{subreddit.url}",
                    "id": subreddit.id,
                    "description": subreddit.description,
                    "public_description": subreddit.public_description,
                    "is_over_18": subreddit.over18,
                    "subscribers_count": subreddit.subscribers,
                    "created_at": subreddit.created_utc,
                }
            )

        return pd.DataFrame(subreddits)

    def get_subreddit(self, subreddit_name: str) -> pd.DataFrame:
        subreddit = self.client.subreddit(subreddit_name)
        data = {
            "display_name": subreddit.display_name,
            "url": f"https://www.reddit.com/{subreddit.url}",
            "id": subreddit.id,
            "description": subreddit.description,
            "public_description": subreddit.public_description,
            "is_over_18": subreddit.over18,
            "subscribers_count": subreddit.subscribers,
            "created_at": subreddit.created_utc,
        }
        return pd.DataFrame([data])

    def get_subreddit_posts_of_where(
        self, subreddit_name: str, category: str, limit: int
    ) -> praw.models.listing.generator.ListingGenerator:
        assert category in ["hot", "new", "top", "controversial", "rising"]

        subreddit = self.client.subreddit(subreddit_name)
        submissions_gen = None
        if category == "hot":
            submissions_gen = subreddit.hot(limit=limit)
        if category == "new":
            submissions_gen = subreddit.new(limit=limit)
        if category == "top":
            submissions_gen = subreddit.top(limit=limit)
        if category == "controversial":
            submissions_gen = subreddit.controversial(limit=limit)
        if category == "rising":
            submissions_gen = subreddit.rising(limit=limit)

        submissions = []
        for submission in submissions_gen:
            submissions.append(
                {
                    "subreddit_name": subreddit_name,
                    "submission_id": submission.id,
                    "title": submission.title,
                    "content": submission.selftext,
                    "author": submission.author.name if submission.author else None,
                    "author_id": submission.author.id if submission.author else None,
                    "url": submission.url,
                    "score": submission.score,
                    "upvote_ratio": submission.upvote_ratio,
                    "num_comments": submission.num_comments,
                    "over_18": submission.over_18,
                    "created_at": submission.created_utc,
                }
            )

        return pd.DataFrame(submissions)
