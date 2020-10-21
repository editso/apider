import argparse
import scheduler
import spider
import json
import multiprocessing
import logging
import storage
import selenium
import sys


__base_config__ = {
    'mysql': {
        'host': None,
        'port': 3306,
        'user': None,
        'password': None,
        'db': None
    },
    'es': {
        "host": None,
        "port": None,
        "scheme": None,
        "user": None,
        "password": None
    },
    'scheduler': {
        'timeout': 6000
    },
    'server': {
        'max_connection': 1,
        'timeout': 6000,
        'bind': '127.0.0.1',
        'listen': 9700
    }
}


class LinkedinAdapter(spider.LinkedinAdapter):

    def __init__(self, *args, **kwargs):
        # self.account = spider.LinkedAccount(*args, **kwargs)
        # self.cache = spider.linkedin_cache(**kwargs)
        # self.storage = spider.ElasticStorage(**kwargs)
        pass

    def get_account(self):
        return self.account

    def get_driver(self):
        chrome_options = selenium.webdriver.ChromeOptions()
        chrome_options.add_experimental_option('w3c', False)
        return selenium.webdriver.Chrome(options=chrome_options)
        # return selenium.webdriver.Remote(command_executor="http://172.16.2.129:4444/wd/hub",
        #                                  options=chrome_options)

    def get_cache(self):
        return self.cache

    def get_storage(self):
        return self.storage


class LinkedinService(scheduler.RemoteService):

    def crawl(self, url):
        with spider.Linkedin(url, LinkedinAdapter()) as linked:
            log = linked.start()
            return scheduler.make_response(log.u_target, code=log.code)


class DispatcherListener(scheduler.DispatchListener):

    def error(self, request, *args, **kwargs):
        # print("error", request)
        pass

    def success(self, res, request, **kwargs):
        print(res)


class ConnectAdapter(scheduler.ConnectorAdapter, scheduler.Verify):

    __servers__ = set()

    def __init__(self, cache, max_server=2):
        self._max_server = max_server
        self._verify = scheduler.ConnectionVerify()
        self._queue = multiprocessing.Queue(self._max_server)
        self._cache: storage.HostStorage = cache
        self.__make_process()

    @scheduler.run_thread()
    def __make_process(self):
        self._poller = scheduler.ProcessPoller(target=self.__loop_get_server, interval=5)
        self._poller.start()

    def __loop_get_server(self):
        if not self.__servers__:
            self.__servers__ = set(self._cache.get(count=100))
        if not self.__servers__:
            return
        for server in self.__servers__:
            try:
                host, port = (server.h_host, server.h_port)
                connector = scheduler.connect(
                    host, port, connector_cls=scheduler.RemoteInvokeConnector)
                connector.set_verify(self._verify)
                if connector.available():
                    self._queue.put(connector)
                else:
                    connector.close()
            except Exception:
                pass
        self.__servers__.clear()

    def has_connector(self):
        return not self._queue.empty()

    def get(self):
        return self._queue.get()


class InvokeListener(scheduler.RemoteInvokeListener):

    def __init__(self, cache):
        self._cache = cache

    def on_start(self):
        return super().on_start()

    def on_stop(self):
        return super().on_stop()

    def on_invoke(self, request, connector):
        return super().on_invoke(request, connector)

    def on_invoke_finish(self, resp):
        return super().on_invoke_finish(resp)

    def on_have(self, server):
        self._cache.push(server.host, server.port, stat=1)

    # def on_full(self, server):
    #     self._cache.push(server.host, server.port, stat=2)


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
            # if self.count == 9:
            #     return False
            # self.count += 1
            return self._url is not None
        except Exception:
            return False

    def next_task(self):
        return scheduler.make_request(
            cls_name=LinkedinService.__name__,
            method_name=LinkedinService.crawl.__name__,
            timeout=10 * 60,
            url=self._url
        )


class MysqlCache(spider.Cache):

    def __init__(self, engine):
        self._mysql = storage.UrlStorage(engine)

    def pop(self):
        return self._mysql.get()

    def push(self):
        pass

    def empty(self):
        pass

    def size(self):
        pass


def load_config(config):
    with open(config, 'r', encoding='utf-8') as c:
        __base_config__.update(json.loads(c.read()))


def run_scheduler(args):
    mysql_engine = storage.make_mysql(**__base_config__['mysql'])
    dispatcher = scheduler.remote_invoke_dispatcher(ConnectAdapter(
        cache=storage.HostStorage(mysql_engine)
    ))
    dispatcher.add_listener(DispatcherListener())
    ts = scheduler.Scheduler()
    ts.register(LinkedinTask(MysqlCache(mysql_engine)))
    ts.add_dispatcher(dispatcher)
    ts.dispatch()


def run_server(args):
    server = __base_config__['server']
    server.update(args)
    mysql_engine = storage.make_mysql(**__base_config__['mysql'])
    server = scheduler.RemoteInvokeServer(
        host=server['bind'],
        port=int(server['listen']),
        invoke_timeout=server['timeout'],
        max_connection=server['max_connection'])
    server.add_listener(InvokeListener(
        cache=storage.HostStorage(mysql_engine)
    ))
    server.add_service(LinkedinService)
    server.start()


def run_show(args):
    print(json.dumps(__base_config__))


def main():
    parser = argparse.ArgumentParser(sys.argv[0].split('.')[0])
    parser.set_defaults(func=run_server)
    sub_parser = parser.add_subparsers()

    s_scheduler = sub_parser.add_parser('scheduler')
    s_scheduler.set_defaults(func=run_scheduler)
    s_scheduler.add_argument('-n', '--max-cache-server', default=2)

    server = sub_parser.add_parser("server")
    server.set_defaults(func=run_server)
    server.add_argument('-n', '--max-connection', default=1)
    server.add_argument('-b', '--bind', default='127.0.0.1')
    server.add_argument('-p', '--listen', default=9700)
    server.add_argument('-t', '--timeout', default=6000)

    other = sub_parser.add_parser('save')
    other.add_argument(
        '-t', '--type', choices=['url', 'host', 'account'], required=True)
    other.add_argument('-d', '--data', required=True)

    parser.add_argument('-d', '--debug', default=False)
    parser.add_argument('-D', '--deamon', default=False)
    parser.add_argument('-c', '--config', required=True)

    show = sub_parser.add_parser('show-config')
    show.set_defaults(func=run_show)

    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    try:
        load_config(args.config)
        args.func(args.__dict__)
    except AttributeError:
        parser.print_help()
    except Exception as e:
        print("ERROR:\n{}\n".format(e))
        parser.print_help()


if __name__ == "__main__":
    main()
