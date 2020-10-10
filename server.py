from scheduler import *

from spider import *

import logging

from selenium import webdriver

from account import AccountManager


logging.basicConfig(level=logging.INFO)
conf = load_json('server.json') or {}

accounts = load_json('account.json') or []


if __name__ == "__main__":
    listen = conf['listen']
    elastic = conf['elasticSearch']
    handler = get_linkedin_handler(elastic, accounts=accounts)
    for port in listen:
        server = Server('0.0.0.0', port, handler=handler)
        server.start()
