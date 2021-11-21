import logging
import os
import pandas as pd
import json
from sqlalchemy import create_engine


def get_engine():
    user = 'user'
    password = os.environ['LOTTO_KEY']
    host = 'localhost'
    database = 'lottoluck'

    return create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}', pool_recycle=3600)


def get_df():
    """
    Gets a DataFrame containing the rows from the prize table that don't exist in prize_stats.

    Returns:
        A DataFrame containing rows that exist in prize but not prize_stats.

    """

    sql_engine = get_engine()
    db_connection = sql_engine.connect()

    df = pd.read_sql('SELECT prize, prize_id FROM prize WHERE prize_id NOT IN (SELECT prize_id FROM prize_stats)', db_connection)
    db_connection.close()

    return df


def get_prize_stats_df(df):
    """
    Takes a DataFrame with prize values and returns a new DataFrame with various calculated values.

    Args:
        df: A DataFrame containing rows that exist in prize but not prize_stats.

    Returns:
        A new DataFrame that contains various calculated values from the prize data.

    """

    # Convert prizes to list
    prize_list = df.values.tolist()

    # Used to store all tickets stats
    prize_stats_df = []

    for prize in prize_list:
        # Keep prize_id to maintain index
        prize_id = prize[1]

        # Convert to JSON
        prize_json = json.loads(prize[0])

        # Create Series for prize amounts and prizes remaining
        prize_amounts = pd.Series(prize_json.keys())
        prizes_remaining = pd.Series(prize_json.values())

        # Remove commas and cast as int
        prizes_remaining = prizes_remaining.str.replace(',', '').astype(int)

        top_prizes_remaining = prizes_remaining[0]

        # Combine prize amounts and prizes remaining into dict
        prize_data = {
            'Prize Amt': prize_amounts,
            'Prizes Rem': prizes_remaining,
        }

        # Create DataFrame from prize data
        prize_df = pd.DataFrame(prize_data)

        # Calculate the sum of remaining prizes
        total_prizes_rem = prize_df.iloc[:, 1].sum()

        prize_stats = {
            'prize_id': prize_id,
            'total_prizes_rem': total_prizes_rem,
            'top_prizes_rem': top_prizes_remaining,
        }

        prize_stats_df.append(prize_stats)

    # Convert data into DataFrame and add id column to beginning
    prize_stats_df = pd.DataFrame(prize_stats_df)

    return prize_stats_df


def insert_df(df, table_name):
    """
    Connects to and inserts a DataFrame into the database.

    Args:
        df: The DataFrame to insert.
        table_name: The table to insert into.

    """

    sql_engine = get_engine()
    db_conn = sql_engine.connect()

    try:
        df.to_sql(table_name, db_conn, if_exists='append', index=False)
    except ValueError as vx:
        logging.error(vx)
    except Exception as ex:
        logging.exception(ex)
    else:
        logging.info(f"Table {table_name} successfully updated.")
    finally:
        db_conn.close()


def main():
    logging.basicConfig(filename='lotto_stats.log', level=logging.INFO, format='%(asctime)s : %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('Started Ticket Stats...')

    df = get_df()

    if not df.empty:
        prize_stats_df = get_prize_stats_df(df)
        insert_df(prize_stats_df, 'prize_stats')
    else:
        logging.info("No new rows to insert.")

    logging.info('Finished Ticket Stats.')


if __name__ == '__main__':
    main()
