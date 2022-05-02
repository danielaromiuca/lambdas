import os
import time
import awswrangler as wr
import pandas as pd
import numpy as np
from read_list import get_ls_from_txt
from clean_tweet_text import clean_text
from check_location import check_location_arg
from count_tags import tags_count_df
from handle_files_s3 import S3BucketClient
from path_constants import (
    BUCKET,
    DATABASE_TABLE,
    GLUE_DATABASE,
    PREFIX_RAW,
    PREFIX_CLEAN_UNGRUOPED,
    PATH_USER_LIST,
    PATH_TAGS_ECONOMY,
    PATH_TAGS_UNCERTANTY,
    PATH_STOP_WORDS,
)


def process_dataframe(tweets_df: pd.DataFrame, stop_words: list, economic_tags: list, uncertainty_tags:list,user_list: set) -> pd.DataFrame:

    # Cleans Text
    cleansed_text = tweets_df.text.fillna("").apply(clean_text, stop_words=stop_words)
    # COunts Tokens
    tokens_count, economic_tokens_count, uncertainty_tokens_count = tags_count_df(
        cleansed_text, economic_tags, uncertainty_tags
    )
    # Validates user Location
    # Check if user in list
    user_in_list = np.array(tweets_df.username.apply(lambda x: x in user_list))
    # Check if loc in arg
    user_loc_in_arg = np.array(tweets_df.location.fillna("").apply(check_location_arg))
    # Check if user in list OR loc in arg. If any, then flag_arg to true
    user_arg_flag = [str(max(x)).lower() for x in zip(user_in_list, user_loc_in_arg)]

    economy_flag = ["true" if x > 0 else "false" for x in economic_tokens_count]
    uncertainty_flag = ["true" if x > 0 else "false" for x in uncertainty_tokens_count]
    created_at = pd.to_datetime(tweets_df.created_at)
    year = created_at.dt.year
    month = created_at.dt.month

    df_data = {
        "flag_economy": economy_flag,
        "flag_uncertainty": uncertainty_flag,
        "flag_country_arg": user_arg_flag,
        "process": tweets_df.process,
        "year": year,
        "month": month,
        "tweet_id": tweets_df.tweet_id,
        "text": tweets_df.text,
        "clean_text": cleansed_text,
        "created_at": tweets_df.created_at,
        "username": tweets_df.username,
        "location": tweets_df.location,
        "coordinates": tweets_df.coordinates,
        "retweet_count": tweets_df.retweet_count,
        "retweeted": tweets_df.retweeted,
        "source": tweets_df.source,
        "favorite_count": tweets_df.favorite_count,
        "favorited": tweets_df.favorited,
        "in_reply_to_status_id_str": tweets_df.in_reply_to_status_id_str,
        "tokens_count": tokens_count,
        "economic_tokens_count": economic_tokens_count,
        "uncertainty_tokens_count": uncertainty_tokens_count,
    }
    tweets_df = pd.DataFrame(data=df_data)
    tweets_df = tweets_df.dropna(
        subset=[
            "flag_economy",
            "flag_uncertainty",
            "flag_country_arg",
            "process",
            "year",
            "month",
        ]
    )

    return tweets_df


if __name__ == "__main__":
    #while True:
    s3_client = S3BucketClient(BUCKET)

    stop_words = get_ls_from_txt(PATH_STOP_WORDS)

    user_list = set(get_ls_from_txt(PATH_USER_LIST))

    economic_tags = get_ls_from_txt(PATH_TAGS_ECONOMY)

    uncertainty_tags = get_ls_from_txt(PATH_TAGS_UNCERTANTY)
    #breakpoint()
    s3_keys = [os.path.join("s3://", BUCKET, key) for key in s3_client.get_files_names(PREFIX_RAW, "parquet")]
    print(s3_keys)
    tweets_df = wr.s3.read_parquet(path=s3_keys).replace(to_replace="None", value=np.nan)
    
    processed_tweets_df = process_dataframe(tweets_df,stop_words, economic_tags, uncertainty_tags, user_list)

    wr.s3.to_parquet(
        df=processed_tweets_df,
        path=f"s3://{BUCKET}/{PREFIX_CLEAN_UNGRUOPED}/",
        dataset=True,
        mode="append",
        database=GLUE_DATABASE,
        table=DATABASE_TABLE,
        partition_cols=[
            "flag_economy",
            "flag_uncertainty",
            "flag_country_arg",
            "process",
            "year",
            "month",
        ],
    )
        #wr.s3.delete_objects(['s3://bucket/key0', 's3://bucket/key1']) 
        #time.sleep(1800)
###Agregar Logging!