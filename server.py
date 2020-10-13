import scheduler
import spider
import logging
import selenium



logging.basicConfig(level=logging.INFO)


accounts = spider.load_json('./account.json')


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

class LinkedinService(object):

    def __init__(self, *args,**kwargs):
        self._adapter = LinkedinAdapter(*args, **kwargs)
        self._cache = spider.linkedin_cache(*args, **kwargs)

    def crawl(self, url):
        with spider.Linkedin(url, self._adapter)  as linked:
            log = linked.start()
            logging.info("crawl finish: {}".format(log))
            if log.code == spider.code['success']:
                self._cache.success({
                    url: spider.remove_url_end(url)
                })
            else:
                self._cache.error({
                    'url': spider.remove_url_end(url)
                })
            return scheduler.make_response(log.target, code=log.code)

elastic = {
    "hosts": ["172.16.2.193"],
    "ports": [9200],
    "scheme": "https",
    "user": "elastic",
    "password": "USA76oBn6ZcowOpofKpS"
}


server = scheduler.RemoteInvokeServer('0.0.0.0', 8080, invoke_timeout=10 * 60)
server.add_service(LinkedinService(**elastic))
server.start()
