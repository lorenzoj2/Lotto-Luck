from bs4 import BeautifulSoup
import urllib.request


def get_ticket_urls():
    """Returns the url for every available scratch off ticket """
    # base url to all scratch off games
    url = "https://www.ohiolottery.com/Games/ScratchOffs"

    # download url as html
    content = urllib.request.urlopen(url).read()

    # html format that is able to be parsed
    page = BeautifulSoup(content, 'html.parser')

    ticket_urls = []

    for ticket in page.find_all(class_='igLandListItem'):
        ticket_urls.append(ticket.find('a')['href'])

    return ticket_urls


def main():
    for ticket in get_ticket_urls():
        print(ticket)


if __name__ == '__main__':
    main()
