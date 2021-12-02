import logging
import os
import requests
import pandas as pd
import pathlib
from sqlalchemy import create_engine


def get_engine():
    user = 'user'
    password = os.environ['LOTTO_KEY']
    host = 'localhost'
    database = 'lottoluck'

    return create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}', pool_recycle=3600)


def get_df():
    """
    Gets a DataFrame containing information about the image links for the tickets.

    Returns:
        A DataFrame containing information about the image links for the tickets.

    """

    sql_engine = get_engine()
    db_connection = sql_engine.connect()

    df = pd.read_sql('SELECT DISTINCT pic, price, ticket_number FROM ticket', db_connection)
    db_connection.close()

    return df


def download_img(df):
    """
    Downloads ticket images if they don't already exist.

    Args:
        df: A DataFrame containing information about the image links for the tickets.

    """

    df['pic_id'] = df['price'].astype(str) + "_" + df['ticket_number'].astype(str)

    img_values = df[['pic', 'pic_id']].values.tolist()

    for img_value in img_values:
        img_path = '/var/www/html/img/' + 'oh_' + img_value[1] + '.jpg'
        file = pathlib.Path(img_path)

        if not file.exists():
            pull_img = requests.get(img_value[0], stream=True)
            if pull_img.ok:
                with open(img_path, 'wb+') as file:
                    file.write(pull_img.raw.read())


def main():
    logging.basicConfig(filename='lotto_img.log', level=logging.INFO, format='%(asctime)s : %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('Started Ticket Images...')

    df = get_df()
    download_img(df)

    logging.info('Finished Ticket Images.')


if __name__ == '__main__':
    main()
