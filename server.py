import scheduler
import spider
import logging
import selenium
import storage


logging.basicConfig(level=logging.INFO)
db_engine = storage.make_mysql('root', '79982473', 'test')
server_cache = storage.HostStorage(db_engine)

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
        print("crawl")
        spider.time.sleep(5)
        # with spider.Linkedin(url, self._adapter)  as linked:
        #     # log = linked.start()
        #     log = spider.make_crawl_log('')
        #     return scheduler.make_response(log.u_target, code=log.code)


class InvokeListener(scheduler.RemoteInvokeListener):

    def on_start(self):
        return super().on_start()

    def on_stop(self):
        return super().on_stop()

    def on_invoke(self, request, connector):
        return super().on_invoke(request, connector)
    
    def on_invoke_finish(self, resp):
        return super().on_invoke_finish(resp)

    def on_have(self, server):
        print('on have')
        server_cache.push(server.host, server.port, stat=1)

    def on_full(self, server):
        print('on full')
        server_cache.push(server.host, server.port, stat=2)


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
    server = scheduler.RemoteInvokeServer('127.0.0.1', 9999, invoke_timeout=10 * 60, max_connection=1)
    server.add_listener(InvokeListener())
    server.add_service(LinkedinService)
    server.start()
