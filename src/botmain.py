import tweepy
import logging
from authenticate import create_api
from filemanager import connect_database
from filemanager import insert_imgs_db
from filemanager import add_dms_db
from filemanager import retrieve_img_db
from filemanager import retrieve_dm_db
import time
import itertools
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

last_update_time = 0

def follow_followers(api): # taken from https://realpython.com/twitter-bot-python-tweepy/#using-tweepy
    logger.info("Following followers")
    for follower in tweepy.Cursor(api.followers).items():
        if not follower.following:
            logger.info(f"Following {follower.name}")
            follower.follow()

def process_dms(api):
    logger.info("Checking dms")
    messages = api.list_direct_messages(count=20)
    dm_list = []
    for dm in messages:
        msg = dm._json # get the json info from the dm object
        id = msg['id'] # get the dm id
        time = msg['created_timestamp'] # get the dm timestamp
        text = msg['message_create']['message_data']['text'].lower() # get the dm text content
        if 'blursed' in text or 'loss' in text or 'connect 4' in text or 'connect four' in text:
            tuple = (id, time, text, 0)
            if int(time) > last_update_time: # only add a message tuple if its not already in the list, and is newer than the last update time
                dm_list.append(tuple)

    return dm_list

def post_meme_status(api, conn, tuple):
    id = None
    text = tuple[2]
    # check which type of meme should be retrieved from the database
    if "blursed" in text:
        logger.info('blursed image requested')
        id = retrieve_img_db(conn, 'blursed')
    elif "loss" in text:
        logger.info('loss image requested')
        id = retrieve_img_db(conn, 'loss')
    elif "connect four" in text or "connect 4" in text:
        logger.info('connect 4 image requested')
        id = retrieve_img_db(conn, 'connect4')
    if id:
        response = api.media_upload(filename=id)
        api.update_status(status='hi', media_ids=[response.media_id])
        logger.info("Updated status with meme")
        cwd = os.getcwd() + "/"
        full_path = os.path.join(cwd, id)
        os.remove(full_path)

def main():
    conn = connect_database()
    if conn is None: # no connection established with database
        logger.info("Failed to connect to database")
        return
    insert_imgs_db(conn) # try to add images from local file system to database
    api = create_api() # authenticate with twitter api
    while True: # check once a minute for updates
            follow_followers(api)
            list_dms = process_dms(api)
            if len(list_dms) > 0:
                last_update_time = list_dms[0][1] # update the last update time with
                add_dms_db(conn, list_dms)
            tuple = retrieve_dm_db(conn)
            if tuple:
                post_meme_status(api, conn, tuple) # post a meme from the requested category
            logger.info("Waiting...")
            time.sleep(60)

if __name__ == "__main__":
    main()
