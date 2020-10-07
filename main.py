from spider import *
from scheduler import *
import logging
import multiprocessing
import asyncio
import queue
import account


@thread()
def server(port):
    handler = RemoteClientHandler(JsonDeCoder(), JsonEnCoder())
    handler.register(Linkin, Linkin())
    service = create_remote_server(port, handler)


class Linkin(object):

    def crawl(self, url, **kwargs):
        with Linkedin(user="+79779293107",
                      password="QWEasd123",
                      storage=storage,
                      **kwargs,
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
        while url and storage.exists('linkedin', query={'target': url}):
            if self.has_task():
                url = self.queue.get()
            else:
                url = None
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
    # logging.basicConfig(level=logging.INFO)
    storage = ElasticStorage(hosts="172.16.2.193",
                             user='elastic',
                             password='USA76oBn6ZcowOpofKpS',
                             ports=9200,
                             scheme='https')
    logging.basicConfig(level=logging.INFO)
    ports = (9999, 9991)
    p_server = multiprocessing.Process(target=start_server, args=ports)
    scheduler = multiprocessing.Process(target=start_scheduler, args=ports)
    p_server.start()
    time.sleep(2)
    scheduler.start()

