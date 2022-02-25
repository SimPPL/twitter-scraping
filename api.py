import requests
import os
import json
import argparse
import pandas as pd
import time
from datetime import datetime as dt
from dotenv import dotenv_values

# Load Bearer Token
config = dotenv_values(".env")
BEARER_TOKEN = config['BEARER_TOKEN']

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

parser.add_argument(
    "--output", type=str, default=".", help="Destination of output json files"
)

args = parser.parse_args()

search_url = "https://api.twitter.com/2/tweets/search/all"
followers_url = "https://api.twitter.com/2/users/{id}/followers"
recent_tweets_url = "https://api.twitter.com/2/users/{id}/tweets"

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
query_params = {
    "query": "(from:TwitterDev url:youtube.com)",
    "expansions": "author_id",
    "tweet.fields": "referenced_tweets,created_at",
}

followers_params = {
    "user.fields": "id,name,username",
    "max_results": 5,
}

recent_tweets_params = {
    "user.fields": "id,name,username",
    "max_results": 5,
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


def search_recent_tweets(url, params, tweet_number):

    response = requests.get(url, auth=bearer_oauth, params=params)
    print("request status: ", response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    json_response = response.json()

    tweets = []

    try:
        no_of_tweets = (
            json_response["meta"]["result_count"]
            if json_response["meta"]["result_count"]
            < len(json_response["includes"]["users"])
            else len(json_response["includes"]["users"])
        )
    except KeyError:
        no_of_tweets = 0

    if no_of_tweets > 0:
        for i in range(no_of_tweets):
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
        # tweets.sort(key=lambda x: x["created_at"])

        cmd = "touch " + f"./tweets/{tweet_number}_tweets.json"
        os.popen(cmd)

        with open(
            f"{args.output}/tweets/{tweet_number}_tweets.json", "a", encoding="utf-8"
        ) as f:
            if no_of_tweets != 0:
                json.dump(tweets, fp=f, indent=4, sort_keys=True)
                f.write("\n")
                print(
                    f"All {no_of_tweets} tweets for {url} are written in: ",
                    f"{args.output}/tweets/{url}_tweets.json",
                )

    return tweets, response.headers


def get_followers(url, user_id, params):

    response = requests.get(url.format(id=user_id),
                            auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception("request status: ",
                        response.status_code, response.text)
    json_response = response.json()

    no_of_followers = json_response["meta"]["result_count"]
    followers = []

    for i in range(no_of_followers):
        followers.append(
            {
                "id": json_response["data"][i]["id"],
                "username": json_response["data"][i]["username"],
            }
        )

    cmd = "touch " + f"./followers/{user_id}_followers.json"
    os.popen(cmd)

    with open(
        f"{args.output}/followers/{user_id}_followers.json", "a", encoding="utf-8"
    ) as f:
        json.dump(followers, fp=f, indent=4, sort_keys=True)
        f.write("\n")
        print(
            f"All {no_of_followers} followers for {user_id} are written in: ",
            f"{args.output}/followers/{user_id}_followers.json",
        )
    return no_of_followers, followers, response.headers


def get_recent_tweets(url, user_id, params):

    response = requests.get(url.format(id=user_id),
                            auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception("request status: ",
                        response.status_code, response.text)
    json_response = response.json()

    no_of_tweets = 0
    tweets = []

    try:
        no_of_tweets = json_response["meta"]["result_count"]
    except KeyError:
        no_of_tweets = 0

    for i in range(no_of_tweets):
        tweets.append({"text": json_response["data"][i]["text"]})

    cmd = "touch " + f"./follower_tweets/{user_id}_follower_tweets.json"
    os.popen(cmd)

    with open(
        f"{args.output}/follower_tweets/{user_id}_follower_tweets.json",
        "a",
        encoding="utf-8",
    ) as f:
        json.dump(tweets, fp=f, indent=4, sort_keys=True)
        f.write("\n")
        print(
            f"All {no_of_tweets} tweets for {user_id} are written in: ",
            f"{args.output}/follower_tweets/{user_id}_follower_tweets.json",
        )

    return no_of_tweets, tweets, response.headers


def control_rate_limit(response_headers):

    print("Controlling Rate Limit")

    requests_remaining = int(response_headers["X-Rate-Limit-Remaining"])
    reset_time = dt.fromtimestamp(int(response_headers["X-Rate-Limit-Reset"]))
    time_now = dt.now()
    time_remaining = (reset_time - time_now).total_seconds()

    print("requests remaining: ", requests_remaining)
    print("time remaining: ", time_remaining)

    if time_remaining >= 0:
        if requests_remaining == 0:
            print(f"Rate Limit Reached. Sleeping for {time_remaining}")
            time.sleep(time_remaining)

        elif requests_remaining <= 5 and time_remaining < 30:
            print(f"Rate Limit Nearing. Sleeping for {time_remaining}")
            time.sleep(time_remaining)


def main():

    url_list = get_misinfo_url_from_csv(args.file, args.url_col)

    if len(url_list) == 0:
        raise ValueError(f"No urls found in the mentioned column")

    flag = 0  # To know when to start controlling rate limit
    flag_t = 0  # To know when to start controlling rate limit

    for idx, url in enumerate(url_list):

        print(f"\n \n ========= {url} ========= \n \n")

        time.sleep(1.1)     # To avoid rate limit for academic api

        query_params["query"] = f"(from:TwitterDev url:{url})"
        tweets, response_header_recent = search_recent_tweets(
            search_url, query_params, idx
        )

        if flag != 0:
            control_rate_limit(response_header_recent)

        flag = 1  # To start rate limit check from next iteration

        for tweet in tweets:

            username = tweet["username"]
            user_id = tweet["user_id"]

            print(f"This url was tweeted by {username}: {user_id}")

            # max_results = 100 in followers_params so at max 100 followers will be returned
            no_of_followers, followers, response_header_followers = get_followers(
                followers_url, user_id, followers_params
            )
            print(f"This user has {no_of_followers} followers ... \n")

            if flag_t != 0:
                control_rate_limit(response_header_followers)

            for follower in followers:

                follower_id = follower["id"]
                # max_results = 100 in recent_tweets_params so at max 100 followers will be returned
                no_of_tweets, tweets, response_header_tweets = get_recent_tweets(
                    recent_tweets_url, follower_id, recent_tweets_params
                )
                print(f"{no_of_tweets} tweets found for this user ... \n")

                if flag_t != 0:
                    control_rate_limit(response_header_tweets)

                flag_t = 1


if __name__ == "__main__":
    main()
