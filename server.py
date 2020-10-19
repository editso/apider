import scheduler
import spider
import logging
import selenium


logging.basicConfig(level=logging.DEBUG)

accounts = {}
try:
    accounts = spider.load_json('./account.json')
except Exception:
    pass

elastic = {
    "hosts": ["172.16.2.193"],
    "ports": [9200],
    "scheme": "https",
    "user": "elastic",
    "password": "USA76oBn6ZcowOpofKpS"
}


class LinkedinAdapter(spider.LinkedinAdapter):

    def __init__(self, *args, **kwargs):
        self.account = spider.LinkedAccount(*args, **kwargs)
        # for account in accounts:
        #     self.account.add(account['account'], account['password'])
        self.cache = spider.linkedin_cache(**kwargs)
        self.storage = spider.ElasticStorage(**kwargs)

    def get_account(self):
        return self.account

    def get_driver(self):
        chrome_options = selenium.webdriver.ChromeOptions()
        chrome_options.add_experimental_option('w3c', False)
        # return selenium.webdriver.Chrome(options=chrome_options)
        return selenium.webdriver.Remote(command_executor="http://172.16.2.129:4444/wd/hub",
                                         options=chrome_options)

    def get_cache(self):
        return self.cache

    def get_storage(self):
        return self.storage


adapter = LinkedinAdapter(**elastic)
cache = spider.linkedin_cache(**elastic)


class LinkedinService(scheduler.RemoteService):

    def __init__(self):
        self._adapter = adapter
        self._cache = cache

    def crawl(self, url):
        with spider.Linkedin(url, self._adapter)  as linked:
            log = linked.start()
            return scheduler.make_response(log.u_target, code=log.code)


class LinkedinAdapter(spider.LinkedinAdapter):

    def __init__(self, *args, **kwargs):
        self.account = spider.LinkedAccount(*args, **kwargs)
        for account in accounts:
            self.account.add(account['account'], account['password'])
        self.cache = spider.linkedin_cache(**kwargs)
        self.storage = spider.ElasticStorage(**kwargs)

    def get_account(self):
        return self.account

    def get_driver(self):
        chrome_options = selenium.webdriver.ChromeOptions()
        chrome_options.add_experimental_option('w3c', False)
        # return selenium.webdriver.Chrome(options=chrome_options)
        return selenium.webdriver.Remote(command_executor="http://172.16.2.129:4444/wd/hub",
        options=chrome_options)

    def get_cache(self):
        return self.cache

    def get_storage(self):
        return self.storage


if __name__ == '__main__':
    server = scheduler.RemoteInvokeServer('127.0.0.1', 8888, invoke_timeout=10 * 60)
    # server.add_service(LinkedinService)
    server.start()
