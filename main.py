import spider
import scheduler
import logging

logging.basicConfig(level=logging.DEBUG)


class RemoteServerAdapter(scheduler.ConnectorAdapter):

    def __init__(self, servers:list):
        self._servers = servers
        self._cur_sock = None
        self._connect = True

    def has_connector(self):
        sock = scheduler.socket.create_connection(('0.0.0.0', 8080))
        self._cur_sock = sock
        return self._cur_sock is not None and self._connect

    def get(self):
        self._connect = False
        return scheduler.RemoteInvokeConnector(self._cur_sock)

    def finish(self, server, task):
        self._connect = True


class DispatcherListener(scheduler.DispatchListener):

    def error(self, request, *args, **kwargs):
        pass
       
    def success(self, *args, **kwargs):
        pass

class LinkedinTask(scheduler.Task):

    def __init__(self, cache, *args, **kwargs):
        self._cache = cache
        self._url = None
        self.count =  2

    def has_task(self):
        try:
            data = self._cache.pop()
            self._url = data['url']
            return self._url is not None
        except Exception:
            return False

    def next_task(self):
        return scheduler.make_request(
            cls_name='LinkedinService',
            method_name='crawl',
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

cache.push({
    'url': 'https://www.linkedin.com/in/theahmadimam/'
})

dispatcher = scheduler.remote_invoke_dispatcher(RemoteServerAdapter([]))
dispatcher.add_listener(DispatcherListener())
ts = scheduler.Scheduler()
ts.register(LinkedinTask(cache))
ts.add_dispatcher(dispatcher)
ts.dispatch()
