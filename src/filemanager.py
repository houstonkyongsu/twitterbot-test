import logging
import mysql.connector
from mysql.connector import Error
import os, os.path
import itertools
import random

logger = logging.getLogger()

def connect_database():
    try:
        file = open("../dblogin.txt","r") # open a local text file containing mysql login details

        mydb = mysql.connector.connect(
          host="localhost",
          user=file.readline(),
          password=file.readline(),
          database="twitterbot"
        ) # connect to the twitterbot database using the credentials from local file
        logger.info("Database connection established")
        file.close()

        return mydb # return the connection
    except IOError:
        print(e)
        print(sys.exc_type)

    return None

def add_dms_db(conn, tuples):
    try:
        cursor = conn.cursor()
        # create a dms table if it doesn't exist
        cursor.execute("CREATE TABLE IF NOT EXISTS dms (id VARCHAR(255) NOT NULL, time VARCHAR(255) NOT NULL, text VARCHAR(255), posted CHAR(1) NOT NULL, PRIMARY KEY (id))")
        sql_insert_query = "INSERT IGNORE INTO dms (id, time, text, posted) VALUES (%s, %s, %s, %s)" # insert if there isn't already an entry in the table

        cursor.executemany(sql_insert_query, tuples) # insert multiple rows into the dms table
        conn.commit()

    except mysql.connector.Error as e:
        print(e)

    finally:
        cursor.close()

def retrieve_dm_db(conn):
    try:
        cursor = conn.cursor()
        sql_select_query = "SELECT * FROM dms WHERE posted = 0 ORDER BY time LIMIT 1" # get all the dms which have a posted value of 0 from the dms table
        cursor.execute(sql_select_query)
        record = cursor.fetchall()
        if len(record) > 0: # if there is at least 1 dm meme request not yet fulfilled
            tuple = record[0]
            sql_update_query = "UPDATE dms SET posted = %s WHERE id = %s" # update the row which was selected, chenging the posted value to 1
            sql_tuple = (1, tuple[0])
            cursor.execute(sql_update_query, sql_tuple) # execute the update
            conn.commit()
            cursor.close()
            print(tuple)
            return tuple

        cursor.close()
        return None

    except mysql.connector.Error as e:
        print(e)


def retrieve_img_db(conn, table):
    try:
        cursor = conn.cursor()
        sql_select_query = "SELECT * FROM " + table + " WHERE posted = 0" # get all the images which have a posted value of 0 from the specified table
        cursor.execute(sql_select_query)
        record = cursor.fetchall()
        if len(record) == 0: # if there is at least one image which has not been posted before
            sql_update_query = "UPDATE " + table + " SET posted = %s"
            cursor.execute(sql_update_query, 0)
            conn.commit()
            sql_select_query = "SELECT * FROM " + table + " WHERE posted = %s" # get all the images which have a posted value of 0 from the specified table
            cursor.execute(sql_select_query, 0)
            record = cursor.fetchall()

        tuple = random.choice(record) # randomly choose one of the rows
        id = tuple[0]
        data = tuple[1]

        sql_update_query = "UPDATE " + table + " SET posted = %s WHERE id = %s" # update the row which was selected, chenging the posted value to 1
        sql_tuple = (1, id)
        cursor.execute(sql_update_query, sql_tuple) # execute the update
        conn.commit()
        cursor.close()
        with open(id, "wb") as imgFile:
            imgFile.write(data)
        return id # return the image id

    except mysql.connector.Error as e:
        print(e)

def insert_imgs_db(conn):
    try:
        cursor = conn.cursor()
        path = '../img/'
        folders = ['loss', 'blursed', 'connect4'] # image folders to import from
        exts = [".jpg",".jpeg",".png"] # acceptable image file types
        for folder in folders:
            image_data = []
            image_names = []
            temp_path = os.path.join(path, folder)
            for f in os.listdir(temp_path):
                ext = os.path.splitext(f)[1]
                if ext.lower() not in exts: # skip unaccepted file types
                    continue
                filename = os.path.join(temp_path,f)
                image_names.append(f) # add filename to list
                with open(filename, 'rb') as file: # convert to binary data
                    binaryData = file.read()
                image_data.append(binaryData) # add binary data to list

            # create a table for each folder of images if it doesn't exist
            cursor.execute("CREATE TABLE IF NOT EXISTS " + folder + " (id VARCHAR(255) NOT NULL, photo BLOB NOT NULL, posted CHAR(1) NOT NULL, PRIMARY KEY (id))")

            tuples = list(zip(image_names, image_data, itertools.repeat(0))) # zip together the lists of image names and data, with an added 0 column
            sql_insert_blob = "INSERT IGNORE INTO " + folder + " (id, photo, posted) VALUES (%s, %s, %s)" # prepared statement to insert rows to the corresponding table

            cursor.executemany(sql_insert_blob, tuples) # insert a row for each tuple in the list
            conn.commit()


    except (IOError, mysql.connector.Error) as e:
        print(e)

    finally:
        cursor.close()
