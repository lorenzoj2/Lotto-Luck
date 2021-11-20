from datetime import datetime
from bs4 import BeautifulSoup
import urllib.request
import pandas as pd
import logging
import json
import os
from sqlalchemy import create_engine
from sqlalchemy import exc


def get_ticket_urls():
    """
    Gets the URLs of every ticket.

    Returns:
        A list of URLs for every available ticket.
    """

    logging.info("Collecting ticket URLs...")

    # base url to all scratch off games
    url = "https://www.ohiolottery.com/Games/ScratchOffs"

    # download url as html
    request = urllib.request.urlopen(url)
    content = request.read()

    # html format that is able to be parsed
    page = BeautifulSoup(content, 'html.parser')

    # list of every ticket's url
    ticket_urls = []

    for ticket in page.find_all(class_='igLandListItem'):
        ticket_urls.append("https://www.ohiolottery.com/" + ticket.find('a')['href'])

    return ticket_urls


def get_ticket(url):
    """

    Gets the available data for a ticket given the URL.

    Args:
        url: The url of the ticket.

    Returns:
         A list containing the ticket's available data.

    """

    # download url as html
    content = urllib.request.urlopen(url).read()

    # html format that is able to be parsed
    page = BeautifulSoup(content, 'html.parser')

    # time data was scraped
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # collect information about the ticket
    ticket_name = page.find('h1').text.strip()

    ticket_number = page.find('span', {'class': 'number'}).text.strip('#')

    ticket_price = url.split('/')[6].strip('$')
    if ticket_price == "20DollarGames":
        ticket_price = "20-Games"
    elif ticket_price == "10DollarGames":
        ticket_price = "10-Games"
    ticket_price = ticket_price.split('-')[0]

    try:
        ticket_odds = page.find(class_='odds').text.strip("Overall odds of winning: ").split()[2]
        ticket_odds = pd.to_numeric(ticket_odds)
    except IndexError:
        ticket_odds = 0.0   # no info available

    ticket_tier = []    # ticket prize amounts
    ticket_rem = []     # ticket prizes remaining

    for game in page.find_all(True, {'class': ['tpdPrizeCell', 'tpdRemainCell']}):
        if game['class'] == ['tpdPrizeCell']:
            ticket_tier.append(game.text.strip())
        else:
            ticket_rem.append(game.text.strip())

    # prize table for the ticket
    ticket_prize = json.dumps(dict(zip(ticket_tier, ticket_rem)))

    # url to ticket's image
    ticket_pic = page.find(class_='igTicketImg')['style']
    ticket_pic = "https://www.ohiolottery.com" + ticket_pic[ticket_pic.find("(") + 1:ticket_pic.find(")")]

    # add ticket information to log
    logging.info([ticket_name, ticket_number, ticket_price])
    return [ticket_name, ticket_number, ticket_price, ticket_odds, ticket_prize, ticket_pic, now]


def get_tickets_df(num_tickets=None):
    """
    Returns a Dataframe of every ticket's information.

    Args:
        num_tickets: Number of tickets to add to the DataFrame.

    Returns:
        A DataFrame containing every tickets information.

    """

    data = []
    ticket_urls = get_ticket_urls()

    for idx, url in enumerate(ticket_urls):
        if num_tickets and idx >= num_tickets:
            break;

        try:
            data.append(get_ticket(url))
        except urllib.error.HTTPERROR as err:
            logging.error(err)

    return pd.DataFrame(data, columns=['name', 'ticket_number', 'price', 'odds', 'prize', 'pic', 'time'])


def get_engine():
    user = 'user'
    password = os.environ['LOTTO_KEY']
    host = 'localhost'
    database = 'lottoluck'

    return create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}', pool_recycle=3600)


def insert_df(df, table_name):
    """

    Connects to and inserts a DataFrame into the database.

    Args:
        df: The DataFrame to insert.
        table_name: The table to insert into.

    """
    sql_engine = get_engine()
    db_connection = sql_engine.connect()

    try:
        df.to_sql(table_name, db_connection, if_exists='append', index=False)
    except exc.IntegrityError:
        logging.error("Integrity Error. Field may already exist.")
        pass
    except ValueError as vx:
        logging.error(vx)
    except Exception as ex:
        logging.exception(ex)
    else:
        logging.info(f"Table {table_name} successfully updated.")
    finally:
        db_connection.close()


def main():
    logging.basicConfig(filename="lotto.log", level=logging.INFO, format='%(asctime)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('Started Scraping...')

    df = get_tickets_df()

    ticket_df = df[['ticket_number', 'name', 'price', 'odds', 'pic']].copy()
    prize_df = df[['ticket_number', 'prize', 'time']].copy()

    # Insert data into ticket table
    insert_df(ticket_df, 'ticket')

    # Insert data into the prize table
    insert_df(prize_df, 'prize')

    logging.info('Finished.')


if __name__ == '__main__':
    main()

