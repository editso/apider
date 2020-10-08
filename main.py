from spider import *
from scheduler import *
import multiprocessing
from selenium import webdriver
import queue
import hashlib
import json
import logging

logging.basicConfig(level=logging.INFO)


conf = load_json('conf.json')



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
        self.queue.put("http://vcdf")
        self.queue.put("1")
        self.queue.put("3")
        self._pop_url = None

    def re_task(self, task_meta):
        self.queue.put(task_meta.get_kwargs()['url'])

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

if __name__ == "__main__":
    data = json.dumps(conf, ensure_ascii=False)
    scheduler = remote_scheduler()
    server = conf['server']
    for s in server:
        scheduler.register(s['host'], s['port'])
    scheduler.add_task(Task())
    scheduler.execute_task()
