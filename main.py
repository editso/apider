import spider
import scheduler
import logging


logging.basicConfig(level=logging.INFO)


class DispatcherListener(scheduler.DispatchListener):

    def error(self, request, *args, **kwargs):
        print(request)

    def success(self, *args, **kwargs):
        pass


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
            if self.count == 2:
                return False
            self.count += 1
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
    remote_server = scheduler.ElasticRemoteServer(["https://elastic:USA76oBn6ZcowOpofKpS@172.16.2.193:9200"])
    adapter = scheduler.remote_connector_adapter(remote_server)
    dispatcher = scheduler.remote_invoke_dispatcher(adapter)
    dispatcher.add_listener(DispatcherListener())
    ts = scheduler.Scheduler()
    ts.register(LinkedinTask(cache))
    ts.add_dispatcher(dispatcher)
    ts.dispatch()
