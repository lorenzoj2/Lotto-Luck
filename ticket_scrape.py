from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup


def get_ticket_hrefs(driver):
    ticket_hrefs = []
    try:
        tickets = driver.find_elements(By.CLASS_NAME, "igLandListItem")

        for ticket in tickets:
            # Convert to HTML
            html_cont = ticket.get_attribute('innerHTML')

            # Parse the HTML content
            soup = BeautifulSoup(html_cont, 'html.parser')

            # Find the <a> tag
            a_tag = soup.find('a')

            if a_tag:
                href_value = a_tag.get('href')
                ticket_hrefs.append(href_value)
                print(href_value)

    except Exception as e:
        print(e)
    finally:
        driver.quit()
        return ticket_hrefs


def main():
    # Set up driver
    driver = webdriver.Chrome()

    # Get web page and wait for data to load
    driver.get('https://www.ohiolottery.com/games/scratch-offs')
    driver.implicitly_wait(1.0)

    ticket_hrefs = get_ticket_hrefs(driver)

    for href in ticket_hrefs:
        print(href)


if __name__ == '__main__':
    main()

