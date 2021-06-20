from urllib.parse import urlparse

import requests
from requests.exceptions import SSLError, ConnectTimeout, ConnectionError
from bs4 import BeautifulSoup


PROXIES = {
    "http": "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050",
}
GLOBAL_UNIQ = set()


def get_first_domain(link):
    parse_result = urlparse(link)
    domain = parse_result.netloc.split(".")[-1]
    return domain


def prepare_link(raw, response):
    href = raw.get("href", "")
    if not href:
        return ""

    link = href
    if "http" not in href:
        parse_result = urlparse(response.request.url)
        link = f"{parse_result.scheme}://{parse_result.netloc}{href}"

    if get_first_domain(link) != "onion":
        return ""
    return link


def fetch_links(response):
    soup = BeautifulSoup(response.content, "html.parser")
    links = []
    for link in soup.find_all("a"):
        link = prepare_link(link, response)
        if link:
            links.append(link)
    return links


def request_page(url):
    return requests.get(url, timeout=20, proxies=PROXIES)


if __name__ == "__main__":
    url = "http://dirnxxdraygbifgc.onion/"
    queue = set()
    for link in fetch_links(request_page(url)):
        queue.add(link)

    while queue:
        print("queue size", len(queue))
        link = queue.pop()
        print("work with", link)
        try:
            page = request_page(link)
        except (SSLError, ConnectionError, ConnectTimeout):
            continue

        for link in fetch_links(page):
            if link in GLOBAL_UNIQ:
                continue

            GLOBAL_UNIQ.add(link)
            queue.add(link)
