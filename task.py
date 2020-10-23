import scheduler
import account
import storage
import spider
import selenium
import multiprocessing
import logging
import time
from selenium import webdriver
from selenium.webdriver.remote.remote_connection import LOGGER
from urllib3.connectionpool import log
from scheduler import __config__


__url_cache_name__ = 'linkedin_cache2'

def get_elasticsearch():
    es = __config__['es']
    return {
        'hosts': es['host'],
        'ports': es['port'],
        'scheme': es['scheme'],
        'user': es['user'],
        'password': es['password']
    }


class AccountCache(account.AccountManager):
    def __init__(self, group, engine):
        self._cache = storage.AccountStorage(engine=engine)
        self._group = group

    def get(self):
        account = self._cache.get(self.group)
        return {
            'account': account.u_account,
            'password': account.u_password
        } if account else None

    def invalid(self, account):
        self._cache.push(self.group, account, stat=2)


class UrlCache(spider.Cache):

    def __init__(self, group, engine) -> None:
        self._engine = engine
        self._cahce = storage.UrlStorage(self._engine)
        self._group = group

    def push(self, value):
        self._cahce.push(group=self._group, **value)

    def pop(self):
        data = self._cahce.get(self._group)
        return data[0].u_target if data else None


class LinkedinAdapter(spider.LinkedinAdapter):

    def __init__(self, es, *args, **kwargs):
        self._cache = spider.ElasticCache(__url_cache_name__, elastic=es)
        self._account = spider.LinkedAccount(**es)
        self._storage = spider.ElasticStorage(**es)

    def get_account(self):
        return self._account

    def get_driver(self):
        LOGGER.setLevel(logging.ERROR)
        log.setLevel(logging.ERROR)
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_experimental_option('w3c', False)
        return webdriver.Chrome(options=chrome_options)
        # return selenium.webdriver.Remote(command_executor="http://172.16.2.129:4444/wd/hub",
        #                                  options=chrome_options)

    def get_cache(self):
        return self._cache

    def get_storage(self):
        return self._storage


class LinkedinService(scheduler.RemoteService):
    def __init__(self):
        self._adapter = LinkedinAdapter(get_elasticsearch())

    def crawl(self, url):
        with spider.Linkedin(url, self._adapter) as linked:
            log = linked.start()
            return scheduler.make_response(log.target, code=log.code)


class InvokeListener(scheduler.RemoteInvokeListener):


    def on_start(self):
        return super().on_start()

    def on_stop(self):
        return super().on_stop()

    def on_invoke(self, request, connector):
        return super().on_invoke(request, connector)

    def on_invoke_finish(self, resp):
        return super().on_invoke_finish(resp)

    # def on_have(self, server):
    #     self._cache.push(server.host, server.port, stat=1)

    # def on_full(self, server):
    #     self._cache.push(server.host, server.port, stat=2)


class LinkedinTask(scheduler.Task, scheduler.DispatchListener):

    def __init__(self, *args, **kwargs):
        # 本地存储
        self._local = spider.LocalCache('linkedin', __config__.get('cache_name', '.cache/cache.json'))
        self._cache = spider.ElasticCache(__config__.get('es_cache_name', __url_cache_name__), elastic=get_elasticsearch())
        self._url = None
        self.count = 0
        self._reset_local()

    def _reset_local(self):
        while self._local.size() > 0:
            self._cache.push(self._local.pop())

    def retry(self, task: scheduler.Request):
        url = task.kwargs.get('url')
        logging.info('retry task: {}'.format(url))
        self._cache.push({'url': url})

    def has_task(self):
        self._url = None
        while True:
            try:
                self._url = self._cache.pop()
                if self._url:
                    self._url = self._url['url']
                    self._local.push({'url': self._url})
                    break
            except Exception as e:
                logging.debug("get task error", exc_info=e)
            time.sleep(5)
        return self._url is not None

    def next_task(self):
        return scheduler.make_request(
            cls_name=LinkedinService.__name__,
            method_name=LinkedinService.crawl.__name__,
            timeout=10 * 60,
            url=self._url
        )

    def error(self, wrapper:scheduler.TaskWrapper,*args, **kwargs):
        request = wrapper.get()
        url = request.kwargs['url']
        logging.info("Linkedin handler error: {}".format(url))
        self._cache.push({
            'url': url
        }, 'failure')
        
    def success(self, resp, request: scheduler.TaskWrapper, *args, **kwargs):
        url = request.get().kwargs['url']
        try:
            logging.info('Linkedin handler success: {}'.format(url))
            self._local.remove({
                "url": url
            })
        except Exception as e:
            logging.debug('Linkedin handler error', exc_info=e)
        self._cache.push({'url': url}, 'success')
