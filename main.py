import spider
import scheduler
import logging
import elasticsearch
import storage
import multiprocessing
from scheduler import Connector

logging.basicConfig(level=logging.INFO)
db_engine = storage.make_mysql('root', '79982473', 'test')
server_cache = storage.HostStorage(db_engine)


class ConnectAdapter(scheduler.ConnectorAdapter, scheduler.Verify):

    __servers__ = set()

    def __init__(self):
        self._verify = scheduler.ConnectionVerify()
        self._queue = multiprocessing.Queue(2)
        self.__make_process()
    
    @scheduler.run_thread()
    def __make_process(self):
        self._poller = scheduler.ProcessPoller(target=self.__loop_get_server, interval=5)
        self._poller.start()

    def __loop_get_server(self):
        if not self.__servers__:
            self.__servers__ = set(server_cache.get(count=5))
        if not self.__servers__:
            return
        for server in self.__servers__:
            try:
                host, port = (server.h_host, server.h_port)
                connector = scheduler.connect(host, port, connector_cls=scheduler.RemoteInvokeConnector)
                connector.set_verify(self._verify)
                if connector.available():
                    self._queue.put(connector)
                else:
                    connector.close()
            except Exception as e:
                pass
        self.__servers__.clear()

    def has_connector(self):
        print("....")
        return not self._queue.empty()

    def get(self):
        print("get")
        return self._queue.get()


class DispatcherListener(scheduler.DispatchListener):

    def error(self, request, *args, **kwargs):
        print("error", request)

    def success(self, res, request, **kwargs):
        print(res)


class LinkedinTask(scheduler.Task):

    def __init__(self, cache, *args, **kwargs):
        self._cache = cache
        self._url = None
        self.count = 0

    def has_task(self):
        try:
            # data = self._cache.pop()
            # self._url = data['url']
            self._url = '11111'
            if self.count == 9:
                return False
            self.count += 1
            print("task")
            return self._url is not None
        except Exception:
            return False

    def next_task(self):
        return scheduler.make_request(
            cls_name='LinkedinService',
            method_name='crawl',
            timeout=10 * 60,
            url=self._url
        )


elastic = {
    "hosts": ["172.16.2.193"],
    "ports": [9200],
    "scheme": "https",
    "user": "elastic",
    "password": "USA76oBn6ZcowOpofKpS"
}

cache = spider.linkedin_cache(**elastic)


if __name__ == '__main__':
    dispatcher = scheduler.remote_invoke_dispatcher(ConnectAdapter())
    dispatcher.add_listener(DispatcherListener())
    ts = scheduler.Scheduler()
    ts.register(LinkedinTask(cache))
    ts.add_dispatcher(dispatcher)
    ts.dispatch()
