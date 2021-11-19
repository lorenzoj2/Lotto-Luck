import logging
import mysql.connector
import os
import pandas as pd
import numpy as np
import json

url = os.environ['TICKETS_IP']

def read_db():
    config = {
        'user': 'user',
        'password': os.environ['LOTTO_KEY'],
        'host': 'localhost',
        'database': 'lottoluck',
        'raise_on_warnings': True,
    }

    try:
        db = mysql.connector.connect(**config)
    except KeyError as err:
        logging.error(err)

    cursor = db.cursor()

    cursor.execute("SELECT * FROM new_ticket")

    result = cursor.fetchall()

    for x in result:
        print(x)


def get_ticket_df():
    df_full = pd.read_json(url)

    print(df_full)


def main():
    logging.basicConfig(filename="lotto_stats.log", level=logging.INFO, format='%(asctime)s : %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('Started Ticket Stats...')

    read_db()

    logging.info('Finished Ticket Stats.')


if __name__ == '__main__':
    main()

