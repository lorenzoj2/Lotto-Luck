from datetime import datetime
from bs4 import BeautifulSoup
import urllib.request
import pandas as pd
import mysql.connector
import os
import logging
import time


def get_ticket_urls():
    """
    Returns a list of URLs for every available scratch off ticket
    """

    logging.info("Collecting ticket URLs...")

    # base url to all scratch off games
    url = "https://www.ohiolottery.com/Games/ScratchOffs"

    # download url as html
    content = urllib.request.urlopen(url).read()

    # html format that is able to be parsed
    page = BeautifulSoup(content, 'html.parser')

    # list of every ticket's url
    ticket_urls = []

    for ticket in page.find_all(class_='igLandListItem'):
        ticket_urls.append("https://www.ohiolottery.com/" + ticket.find('a')['href'])

    return ticket_urls


def get_ticket(url):
    """
    Returns the available data for a scratch off given the URL
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
    ticket_prize = dict(zip(ticket_tier, ticket_rem))

    # url to ticket's image
    ticket_pic = page.find(class_='igTicketImg')['style']
    ticket_pic = "https://www.ohiolottery.com" + ticket_pic[ticket_pic.find("(") + 1:ticket_pic.find(")")]

    # add ticket information to log
    logging.info([ticket_name, ticket_number, ticket_price, ticket_odds, ticket_prize, ticket_pic, now])
    return [ticket_name, ticket_number, ticket_price, ticket_odds, ticket_prize, ticket_pic, now]


def get_tickets_df():
    """
    Returns a Dataframe of every ticket's information
    """

    data = []

    for url in get_ticket_urls():
        data.append(get_ticket(url))
        time.sleep(5)

    return pd.DataFrame(data, columns=['Name', 'Number', 'Price', 'Odds', 'Prize', 'Pic', 'Time'])


def update_db(df):
    """
    Inserts new records into the ticket table
    """

    logging.info("Inserting new records into database...")

    config = {
        'user': 'root',
        'password': os.environ['LOTTO_KEY'],
        'host': 'localhost',
        'database': 'lottoluck',
        'raise_on_warnings': True,
    }

    db = mysql.connector.connect(**config)

    cursor = db.cursor()

    for index, row in df.iterrows():
        query = f"INSERT INTO lottoluck.ticket (name, number, price, odds, prize, pic, time) " \
                f"VALUES (\"{row['Name']}\", {row['Number']}, {row['Price']}, \"{row['Odds']}\", \"{(row['Prize'])}\", \"{row['Pic']}\", \"{row['Time']}\");"

        try:
            cursor.execute(query)
            db.commit()
        except mysql.connector.Error as err:
            logging.error(err, query)


def main():
    logging.basicConfig(filename="lotto.log", level=logging.INFO, format='%(asctime)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('Started Scraping...')

    df = get_tickets_df()
    update_db(df)

    logging.info('Finished.')


if __name__ == '__main__':
    main()
