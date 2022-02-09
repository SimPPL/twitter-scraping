import requests
import os
import json
import argparse
import pandas as pd
from config import BEARER_TOKEN

# Load file address of misinformation csv file
parser = argparse.ArgumentParser(description="simppl misinformation urls")
parser.add_argument(
    "--file", type=str, required=True, help="location of the url csv file"
)
parser.add_argument(
    "--url_col",
    type=str,
    default="url",
    required=True,
    help="name of column containing misinformation urls",
)
args = parser.parse_args()

search_url = "https://api.twitter.com/2/tweets/search/recent"
followers_url = "https://api.twitter.com/2/users/{id}/followers"
recent_tweets_url = "https://api.twitter.com/2/users/{id}/tweets"

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
query_params = {
    "query": "(url:youtube.com)",
    "expansions": "author_id",
    "tweet.fields": "referenced_tweets,created_at",
}

followers_params = {
    "user.fields": "id,name,username",
    "max_results": 100,
}

recent_tweets_params = {
    "user.fields": "id,name,username",
    "max_results": 100,
    "tweet.fields": "referenced_tweets,created_at",
}


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r


def get_misinfo_url_from_csv(file_name, target_col="url"):
    """
    :file_name: Name of the file containing misinformation urls (csv format)
    :target_col: Column name in the csv file which consists the urls (default='url')
    """
    df = pd.read_csv(file_name)
    url_list = list(df[target_col].unique())
    return url_list


def search_recent_tweets(url, params):

    response = requests.get(url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    json_response = response.json()

    tweets = []

    for i in range(json_response["meta"]["result_count"]):
        if (
            "referenced_tweets" not in json_response["data"][i]
            and json_response["data"][i]["author_id"]
            == json_response["includes"]["users"][i]["id"]
        ):
            tweets.append(
                {
                    "tweet_id": json_response["data"][i]["id"],
                    "user_id": json_response["includes"]["users"][i]["id"],
                    "username": json_response["includes"]["users"][i]["username"],
                    "name": json_response["includes"]["users"][i]["name"],
                    "created_at": json_response["data"][i]["created_at"],
                }
            )
    tweets.sort(key=lambda x: x["created_at"])

    with open("data.json", "a", encoding="utf-8") as f:
        if len(tweets) != 0:
            json.dump(tweets, fp=f, indent=4, sort_keys=True)
            f.write("\n")
            print("tweets written to file")

    return tweets


def get_followers(url, user_id, params):

    response = requests.get(url.format(id=user_id), auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    json_response = response.json()

    no_of_followers = json_response["meta"]["result_count"]
    followers = []

    for i in range(no_of_followers):
        followers.append(json_response["data"][i]["id"])
    with open("followers.json", "a", encoding="utf-8") as f:
        json.dump(json_response, fp=f, indent=4, sort_keys=True)
        f.write("\n")

    return no_of_followers, followers


def get_recent_tweets(url, user_id, params):

    response = requests.get(url.format(id=user_id), auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    json_response = response.json()

    no_of_tweets = json_response["meta"]["result_count"]
    tweets = []

    for i in range(no_of_tweets):
        tweets.append(json_response["data"][i]["text"])
    with open("recent_tweets.json", "a", encoding="utf-8") as f:
        json.dump(json_response, fp=f, indent=4, sort_keys=True)
        f.write("\n")

    return no_of_tweets, tweets


def main():
    url_list = get_misinfo_url_from_csv(args.file, args.url_col)
    if len(url_list) == 0:
        raise ValueError(f"No urls found in the mentioned column")

    for url in url_list:

        query_params["query"] = f"(url:{url})"
        tweets = search_recent_tweets(search_url, query_params)
        print(f"{url} tweets: {len(tweets)}")

        for tweet in tweets:

            # max_results = 100 in followers_params so at max 100 followers will be returned
            no_of_followers, followers = get_followers(
                followers_url, tweet["user_id"], followers_params
            )
            print(no_of_followers)

            # max_results = 100 in recent_tweets_params so at max 100 followers will be returned
            no_of_tweets, tweets = get_recent_tweets(
                recent_tweets_url, tweets[0]["user_id"], recent_tweets_params
            )
            print(no_of_tweets)


if __name__ == "__main__":
    main()
