import tweepy
import logging
from login import create_api
import time
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def follow_followers(api):
    logger.info("Following followers")
    for follower in tweepy.Cursor(api.followers).items():
        if not follower.following:
            logger.info(f"Following {follower.name}")
            follower.follow()

def respond_dms(api):
    logger.info("Replying to dms")
    for dm in tweepy.Cursor(api.list_direct_messages(10,-1)):
        if contains_word(dm, "hi"):
            print("yooo \n")

def contains_word(msg, word):
    msg = str(msg)
    try:
        msg = msg.replace("\\","")
        dict_msg = json.loads(msg)
        # if 'message_create' in dict_msg.keys():
        print(dict_msg)

    except:
        pass

    return True

def main():
    api = create_api()
    while True:
        follow_followers(api)
        logger.info("Waiting...")
        time.sleep(60)

if __name__ == "__main__":
    main()
