import logging
import os
import pandas as pd
import json
import numpy as np
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

    df = pd.read_sql('SELECT DISTINCT price, odds, prize, prize_id FROM prize NATURAL JOIN ticket WHERE prize_id NOT IN (SELECT prize_id FROM prize_stats)', db_connection)
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
    prize_list = df[['prize', 'prize_id', 'price', 'odds']].values.tolist()

    # Used to store all tickets stats
    prize_stats_df = []

    for prize in prize_list:
        # Convert to JSON
        prize_json = json.loads(prize[0])

        # Keep prize_id to maintain index
        prize_id = prize[1]

        # Use price and odds for estimated value
        price = prize[2]
        odds = float(prize[3])

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

        # Calculate the estimated value of a ticket
        ev_score = get_ev_score(prize_amounts, prizes_remaining, price, odds)

        prize_stats = {
            'prize_id': prize_id,
            'total_prizes_rem': total_prizes_rem,
            'top_prizes_rem': top_prizes_remaining,
            'ev_score': ev_score,
        }

        prize_stats_df.append(prize_stats)

    # Convert data into DataFrame and add id column to beginning
    prize_stats_df = pd.DataFrame(prize_stats_df)

    # Normalize ev_score to values between 1 and 10
    prize_stats_df['ev_score'] = (prize_stats_df['ev_score'] - prize_stats_df['ev_score'].min()) / np.ptp(prize_stats_df['ev_score']) * 9 + 1

    return prize_stats_df


def get_ev_score(prize_amounts, prizes_remaining, price, odds):
    """
    Gets an estimated value score based on the ticket's price, odds, and prizes remaining.

    Args:
        prize_amounts: Series containing prize amounts.
        prizes_remaining: Series containing prizes remaining.
        price: Ticket's price.
        odds: Ticket's odds of winning.

    Returns:
        A calculated estimated value score.

    """

    new_prize_amounts = []

    for amt in prize_amounts:
        try:
            amt = amt.replace(',', '')
            amt = amt.replace('$', '')
            amt = pd.to_numeric(amt)
        except ValueError:
            amt = amt.upper()

            if '&' in amt:
                # TPD ENTRY & 5500
                amt = amt.split('&')
                amt = amt[1]
                amt = pd.to_numeric(amt)

            elif 'TPD' in amt or 'MEGAPLIER' in amt or 'ENTRY' in amt:
                # ex. 250K/YR FOR LIFE/TPD, top prize drawing, not counted as a prize
                amt = 0

            elif 'LIFE' in amt:
                # ex. 250K/YR FOR LIFE
                amt = amt.split()
                amt = amt[0].split('/')[0].strip("K")
                amt = pd.to_numeric(amt)

                # 20 years worth of prizes
                amt = amt * 1000 * 20

            elif 'FOR' in amt:
                # ex. 2500/MO FOR 10YRS
                amt = (amt.split('FOR'))

                # 10YRS
                time = amt[1].split()[0]
                time = pd.to_numeric(time)

                # 2500
                val = amt[0].split('/')[0]
                # MO
                period = amt[0].split('/')[1]

                if 'K' in val:
                    val = val.strip("K")
                    val = pd.to_numeric(val)
                    val *= 1000
                else:
                    val = pd.to_numeric(val)

                # Change val to represent the amount for one year
                if 'MO' in period:
                    val = pd.to_numeric(val)
                    val *= 12

                amt = val * time

            else:
                logging.info("Value Error while transforming prize amounts.")
                amt = 0

        finally:
            new_prize_amounts.append(amt)

    # Create DataFrame to calculate expected value
    ev = pd.DataFrame(columns=['prize', 'rem', 'x', 'p(x)', 'x * p(x)'])
    ev['prize'] = new_prize_amounts
    ev['rem'] = prizes_remaining
    ev['x'] = ev['prize'] - price
    ev['x'] = ev['x'].clip(0)  # set all negative values to 0
    ev['p(x)'] = ev['rem'] / ev['rem'].sum()
    ev['x * p(x)'] = ev['x'] * ev['p(x)']

    expected = ev['x * p(x)'].sum()

    # If no odds are found use default value
    if odds == 0:
        odds = 4

    # The average return from each ticket
    ev_score = ((expected / odds) - price) / price

    return ev_score


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
