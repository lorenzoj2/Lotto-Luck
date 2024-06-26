import json
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import mysql.connector
import pandas as pd
import os
import logging

logging.basicConfig(filename="lotto.log", level=logging.INFO, format="%(asctime)s : %(message)s",
                    datefmt="%m/%d/%Y %I:%M:%S %p")


def get_driver():
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument(
        f'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_1) AppleWebKit/537.36 (KHTML, like Gecko) '
        f'Chrome/87.0.4280.88 Safari/537.36')
    service = Service(executable_path='/usr/bin/chromedriver')
    driver = webdriver.Chrome(service=service, options=chrome_options)

    return driver


def get_ticket_hrefs():
    """
    Gets the href for every ticket.

    Returns:
        A list of hrefs for every available ticket.
    """

    logging.info("Getting ticket URLS...")

    # Get web page and wait for data to load
    driver = get_driver()
    driver.get("https://www.ohiolottery.com/games/scratch-offs")
    time.sleep(2)

    ticket_hrefs = []
    try:
        tickets = driver.find_elements(By.CLASS_NAME, "igLandListItem")

        for ticket in tickets:
            # Convert to HTML
            html_cont = ticket.get_attribute("innerHTML")

            # Parse the HTML content
            soup = BeautifulSoup(html_cont, "html.parser")

            # Find the <a> tag
            a_tag = soup.find("a")
            href_value = a_tag.get("href")

            ticket_hrefs.append(href_value)

    except Exception as e:
        logging.error("Unable to collect ticket URLS.")
        logging.error(e)

    finally:
        driver.close()
        return ticket_hrefs


def get_ticket_info(href):
    """
      Gets the available data for a scratch off ticket.

      Args:
          href: The href of the ticket.

      Returns:
           A list containing the ticket's available data.

      """

    logging.info(f"Getting ticket information ({href})")

    # Get web page and wait for data to load
    driver = get_driver()
    driver.get(f"https://www.ohiolottery.com{href}")
    time.sleep(2)

    # Create a dictionary with all fields to collect for a ticket
    ticket_data = {
        "ticket_name": "",
        "ticket_number": "",
        "ticket_price": "",
        "ticket_odds": "",
        "ticket_prize": "",
        "now": ""
    }

    try:
        # Ticket Name
        ticket_data["ticket_name"] = driver.find_element(By.CSS_SELECTOR, "H1").text

        # Ticket Number
        ticket_number = driver.find_element(By.CLASS_NAME, "number").text
        ticket_data["ticket_number"] = ticket_number.strip("#")

        # Ticket Price
        ticket_price = href.split("/")[3]
        ticket_data["ticket_price"] = ticket_price.strip("$").split("-")[0]

        # Ticket Odds
        ticket_odds = driver.find_element(By.CLASS_NAME, "odds").text
        ticket_data["ticket_odds"] = ticket_odds.split(" ")[-1]

        # Ticket Prize     
        ticket_tier = []  # ticket prize amounts
        ticket_rem = []  # ticket prizes remaining

        # Ticket Prize
        prize_table = driver.find_element(By.CLASS_NAME, "tbl_PrizesRemaining")
        prizes = prize_table.find_elements(By.CLASS_NAME, "grid-x")

        for i, prize in enumerate(prizes):
            # Exclude header columns
            if i > 1:
                prize_split = prize.text.splitlines()

                ticket_tier.append(prize_split[0])
                ticket_rem.append(prize_split[1])

        ticket_prize = json.dumps(dict(zip(ticket_tier, ticket_rem)))
        ticket_data["ticket_prize"] = ticket_prize

        # Collection DateTime
        ticket_data["now"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    except Exception as e:
        logging.error(f"Unable to fetch ticket data: {href}\n{e}")
        return None

    finally:
        driver.close()
        return ticket_data


def get_ticket_df(num_tickets=None):
    """
    Returns a Dataframe of every ticket's information.

    Args:
        num_tickets: Number of tickets to add to the DataFrame.

    Returns:
        A DataFrame containing information for every ticket

    """

    data = []
    ticket_hrefs = get_ticket_hrefs()

    for i, href in enumerate(ticket_hrefs):
        if num_tickets and i >= num_tickets:
            break

        try:
            ticket_data = get_ticket_info(href)
            data.append(ticket_data.values())

        except Exception as e:
            logging.error("Unable to return ticket DataFrame.")
            logging.error(e)

    return pd.DataFrame(data, columns=['name', 'ticket_number', 'price', 'odds', 'prize', 'time'])


def get_conn():
    conn = mysql.connector.connect(
        host='localhost',
        user='user',
        password=os.environ['LOTTO_KEY'],
        database='lottoluck',
    )

    return conn


def insert_df(df, table_name, ignore=False):
    """
    Connects to and inserts a DataFrame into the database.

    Args:
        df: The DataFrame to insert.
        table_name: The table to insert into.
        ignore: Whether to ignore existing records in table when inserting.

    """

    conn = get_conn()
    cursor = conn.cursor(prepared=True)
    rows_affected = 0

    query = (f'INSERT {"IGNORE " if ignore else ""} '
             f'INTO {table_name}({", ".join(df.columns)}) '
             f'VALUES({", ".join(["? " for _ in df.columns])})')

    for x in df.to_dict(orient='split')['data']:
        try:
            # Insert new data
            cursor.execute(query, x)
            rows_affected += cursor.rowcount
        except mysql.connector.Error as err:
            logging.error(err)

    # Commit data to database
    conn.commit()

    logging.info(f'{table_name} : {rows_affected} rows successfully updated.')

    cursor.close()
    conn.close()


def main():
    logging.info("Started scraping...")

    try:
        df = get_ticket_df()

        ticket_df = df[['ticket_number', 'name', 'price', 'odds', 'pic']].copy()
        prize_df = df[['ticket_number', 'prize', 'time']].copy()

        # Insert data into ticket table
        insert_df(ticket_df, 'ticket', ignore=True)

        # Insert data into the prize table
        insert_df(prize_df, 'prize')

    except Exception as e:
        logging.error(e)

    logging.info("Finished scraping.")


if __name__ == "__main__":
    main()
