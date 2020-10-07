from spider import *
from scheduler import *
import logging
import multiprocessing
import asyncio
import queue


@thread()
def server(port):
    handler = RemoteClientHandler(JsonDeCoder(), JsonEnCoder())
    handler.register(Linkin, Linkin())
    service = create_remote_server(port, handler)


class Linkin(object):

    def crawl(self, url):
        with Linkedin(user="79982473@qq.com",
                      password="@12345678@",
                      storage=storage,
                      cache=QueueCache(),
                      page=url) as linkedin:
            linkedin.start()


class Task(Task):

    def __init__(self):
        self.queue = queue.Queue()
        self.queue.put("https://www.linkedin.com/in/theahmadimam/")
        # self.queue.put("https://www.linkedin.com/in/williamhgates/")
        # self.queue.put("https://www.linkedin.com/in/williamhgates/")
        # self.queue.put("https://www.linkedin.com/in/williamhgates/")
        # self.queue.put("https://www.linkedin.com/in/williamhgates/")
        # self.queue.put("https://www.linkedin.com/in/williamhgates/")

    def has_task(self):
        return self.queue.qsize() > 0

    def get_task(self):
        url = self.queue.get()
        return TaskInfo(Linkin.__name__, Linkin.crawl.__name__, url=url)


def start_server(*ports):
    for port in ports:
        server(port)


def start_scheduler(*remotes):
    scheduler = remote_scheduler()
    for port in remotes:
        scheduler.register('0.0.0.0', port)
    scheduler.add_task(Task())
    scheduler.execute_task()
    logging.info("scheduler start success")


if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO)
    storage = ElasticStorage(host="172.16.2.193",
                             user='elastic',
                             password='USA76oBn6ZcowOpofKpS',
                             port=9200,
                             schema='https')
    # print(storage.url())

    # storage.save('test', {
    #     'name': 'test'
    # })

    logging.basicConfig(level=logging.INFO)
    ports = (8003, 8004, 8005)

    p_server = multiprocessing.Process(target=start_server, args=ports)
    p_server.start()
    time.sleep(2)
    scheduler = multiprocessing.Process(target=start_scheduler, args=ports)
    scheduler.start()