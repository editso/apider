from spider import *
from scheduler import *
import logging
import multiprocessing
from selenium import webdriver
import asyncio
import queue
import account
import os.path as path


@thread()
def server(port):
    handler = RemoteClientHandler(JsonDeCoder(), JsonEnCoder())
    handler.register(Linkin, Linkin())
    service = create_remote_server(port, handler)


class Linkin(object):

    def crawl(self, url, **kwargs):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_experimental_option('w3c', False)
        driver = webdriver.Remote(
            command_executor='http://172.16.2.129:4444/wd/hub',
            options=chrome_options
        )

        with Linkedin(user="+79777962843",
                      password="@12345678@",
                      storage=storage,
                      **kwargs,
                      driver=driver,
                      cache=QueueCache(),
                      page=url) as linkedin:
            linkedin.start()


class Task(Task):

    def __init__(self):
        self.queue = queue.Queue()
        self.queue.put("https://www.linkedin.com/in/theahmadimam/")
        self._pop_url = None

    def has_task(self):
        url = self.queue.get()
        # while url and storage.exists('linkedin', query={'target': url}):
        #     if self.has_task():
        #         url = self.queue.get()
        #     else:
        #         url = None
        self._pop_url = url
        return url is not None

    def get_task(self):
        return TaskInfo(Linkin.__name__, Linkin.crawl.__name__, url=self._pop_url, debug=True)


def start_server(*ports):
    for port in ports:
        server(port)


def start_scheduler(*remotes):
    scheduler = remote_scheduler()
    for port in remotes:
        scheduler.register('0.0.0.0', port)
    scheduler.add_task(Task())
    scheduler.execute_task()


if __name__ == "__main__":
    conf = {
        'hosts': "172.16.2.193",
        'user': 'elastic',
        'password': 'USA76oBn6ZcowOpofKpS',
        'ports': 9200,
        'scheme': 'https'
    }
    with ElasticCache("cache", 'url', **conf) as es:
        storage = ElasticStorage(**conf)
    # print('ex: ',storage.index_exists('cache'))
    #     es.push({
    #         'url': remove_url_end('https://www.linkedin.com/in/theahmadimam/')
    #     })
        es.push({
            'url': remove_url_end('https://www.linkedin.com/in/reidhoffman/')
        })
        print(es.pop())
        ports = (9999, 9991)
        p_server = multiprocessing.Process(target=start_server, args=ports)
        scheduler = multiprocessing.Process(target=start_scheduler, args=ports)
    # p_server.start()
        time.sleep(2)
    # scheduler.start()
