from typing import List, Dict
import base64
from io import BytesIO

import matplotlib.pyplot as plt

from dagster import (
    AssetExecutionContext,
    MetadataValue,
    asset,
    get_dagster_logger,
)
from .resources import DataGeneratorResource

import pandas as pd  # Add new imports to the top of `assets.py`

import requests


@asset
def topstory_ids() -> List:  # modify return type signature
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_new_story_ids = requests.get(newstories_url).json()[:100]

    return (
        top_new_story_ids  # return top_new_story_ids and the I/O manager will save it
    )


@asset(
    group_name="hackernews",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def topstories(context: AssetExecutionContext, topstory_ids: List) -> pd.DataFrame:
    logger = get_dagster_logger()

    results = []
    for item_id in topstory_ids:
        item = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
        ).json()
        results.append(item)

        if len(results) % 20 == 0:
            logger.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results)

    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )

    return df  # Database_io_manager will save this table in DataBase now


@asset  # remove deps parameter
def most_frequent_words(
    context: AssetExecutionContext,
    topstories: pd.DataFrame,  # add topstories as a function argument
) -> Dict:  # modify the return type signature
    stopwords = ["a", "the", "an", "of", "to", "in", "for", "and", "with", "on", "is"]

    # remove manually loading topstory_ids

    word_counts = {}
    for raw_title in topstories["title"]:
        title = raw_title.lower()
        for word in title.split():
            cleaned_word = word.strip(".,-!?:;()[]'\"-")
            if cleaned_word not in stopwords and len(cleaned_word) > 0:
                word_counts[cleaned_word] = word_counts.get(cleaned_word, 0) + 1

    top_words = {
        pair[0]: pair[1]
        for pair in sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:25]
    }

    plt.figure(figsize=(10, 6))
    plt.bar(list(top_words.keys()), list(top_words.values()))
    plt.xticks(rotation=45, ha="right")
    plt.title("Top 25 Words in Hacker News Titles")
    plt.tight_layout()

    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())

    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # remove manually saving top_words

    context.add_output_metadata(metadata={"plot": MetadataValue.md(md_content)})

    return top_words  # return top_words and the I/O manager will save it


@asset
def signups(
    context: AssetExecutionContext, hackernews_api: DataGeneratorResource
) -> pd.DataFrame:
    signups = pd.DataFrame(hackernews_api.get_signups())

    context.add_output_metadata(
        metadata={
            "Record Count": len(signups),
            "Preview": MetadataValue.md(signups.head().to_markdown()),
            "Earliest Signup": signups["registered_at"].min(),
            "Latest Signup": signups["registered_at"].max(),
        }
    )

    return signups
