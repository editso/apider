import scheduler
import logging
import time
import spider

logging.basicConfig(level=logging.DEBUG)


@scheduler.run_thread()
def run():
    server = scheduler.RemoteInvokeServer('0.0.0.0', 8888)
    server.add_service(Task())
    server.start()


class Adapter(scheduler.ConnectorAdapter):

    def has_connector(self):
        return True

    def get(self):
        sock = scheduler.socket.create_connection(('0.0.0.0', 8888))
        return scheduler.RemoteInvokeConnector(sock)

  
class Listener(scheduler.DispatchListener):

    def error(self, *args, **kwargs):
        print("failure")

    def success(self, *args, **kwargs):
        print(args)


class Task(scheduler.Task):
    def __init__(self):
        self._queue = scheduler.Queue()
        self._queue.put("htt")
        # self._queue.put("wwww")
        # self._queue.put("wwww")
        # self._queue.put("wwww")

    def has_task(self):
        return self._queue.qsize() > 0

    def test(self):
        time.sleep(10)
        return 10

    def next_task(self):
        self._queue.get()
        return scheduler.make_request("Task", "test", timeout=12)


run()

dispatcher = scheduler.remote_invoke_dispatcher(Adapter())
dispatcher.add_listener(Listener())
s = scheduler.Scheduler()
s.add_dispatcher(dispatcher)
s.register(Task())
s.dispatch()

elastic = {
    "hosts": ["172.16.2.193"],
    "ports": [9200],
    "scheme": "https",
    "user": "elastic",
    "password": "USA76oBn6ZcowOpofKpS"
}

# es = spider.ElasticStorage(**elastic)
# sc = spider.ElasticCache("linkedin_cache", elastic=elastic)
# print(es.exists('linkedin', 'ohuBGnUBJQDNASgvoUZD'))
# print(sc.pop())
# sc.reset_stat('linkedin_cache', ['queue', 'success'], 'wait')

