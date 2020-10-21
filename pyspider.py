from os import read
import account
import argparse
import scheduler
import spider
import json
import multiprocessing
import logging
import storage
import selenium
import sys
import getpass
import time

from storage import HostStorage


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

    def __init__(self, engine, *args, **kwargs):
        self._account = AccountCache('linkedin', engine)
        self._cache = UrlCache('linkedin', engine)
        es = __base_config__['es']
        self._storage = spider.ElasticStorage(
            hosts=es['host'],
            ports=es['port'],
            scheme=es['scheme'],
            user=es['user'],
            password=es['password']
        )

    def get_account(self):
        return self._account

    def get_driver(self):
        chrome_options = selenium.webdriver.ChromeOptions()
        chrome_options.add_experimental_option('w3c', False)
        return selenium.webdriver.Chrome(options=chrome_options)
        # return selenium.webdriver.Remote(command_executor="http://172.16.2.129:4444/wd/hub",
        #                                  options=chrome_options)

    def get_cache(self):
        return self._cache

    def get_storage(self):
        return self._storage


class LinkedinService(scheduler.RemoteService):
    def __init__(self):
        self._engine = storage.make_mysql(**__base_config__['mysql'])
        self._adapter = LinkedinAdapter(self._engine)

    def crawl(self, url):
        return ''
        # with spider.Linkedin(url, self._adapter) as linked:
        #     log = linked.start()
        #     if log.code == 200:
        #         self._adapter.get_cache().push({
        #             'url': url,
        #             'stat': 2
        #         })
        #     return scheduler.make_response(log.u_target, code=log.code)


class DispatcherListener(scheduler.DispatchListener):

    def error(self, request, *args, **kwargs):
        print("error", request)

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
        self._poller = scheduler.ProcessPoller(
            target=self.__loop_get_server, interval=5)
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

    # def on_have(self, server):
    #     self._cache.push(server.host, server.port, stat=1)

    # def on_full(self, server):
    #     self._cache.push(server.host, server.port, stat=2)


class LinkedinTask(scheduler.Task):

    def __init__(self, cache, *args, **kwargs):
        self._cache = cache
        self._url = None
        self.count = 0

    def retry(self, task: scheduler.Request):
        url = task.kwargs.get('url')
        logging.info('retry task: {}'.format(url))
        self._cache.push({
            'url': url
        })

    def has_task(self):
        self._url = None
        while True:
            try:
                self._url = self._cache.pop()
                if self._url:
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


def load_config(config):
    with open(config, 'r', encoding='utf-8') as c:
        __base_config__.update(json.loads(c.read()))


def run_scheduler(args):
    load_config(args['config'])
    mysql_engine = storage.make_mysql(**__base_config__['mysql'])
    dispatcher = scheduler.remote_invoke_dispatcher(ConnectAdapter(
        cache=storage.HostStorage(mysql_engine)
    ))
    dispatcher.add_listener(DispatcherListener())
    ts = scheduler.Scheduler()
    ts.register(LinkedinTask(UrlCache('linkedin', mysql_engine)))
    ts.add_dispatcher(dispatcher)
    ts.dispatch()


def run_server(args):
    load_config(args['config'])
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


def make_client(args):
    return storage.make_mysql(input('Enter user: '),
                              getpass.getpass("Enter password: "),
                              args['database'] or input('Enter database: '),
                              args['client_host'] or input('Enter host: '),
                              args['client_port'])


def run_url(args):
    cache = UrlCache(args['group'], make_client(args))
    cache.push({
        'url': args['name']
    })


def run_account(args):
    cache = AccountCache(args['group'], make_client(args))
    cache.add(args['account'], args['password'])


def run_host(args):
    cache = HostStorage(make_client(args))
    cache.push(args['host'], args['port'])


def run_show(args):
    print(json.dumps(__base_config__))


def main():
    parser = argparse.ArgumentParser(sys.argv[0].split('.')[0])
    parent = argparse.ArgumentParser(add_help=False)

    parent.add_argument('-D', '--deamon', default=False)
    parent.add_argument('-d', '--debug', default=False, type=bool)
    parent.add_argument('-c', '--config', required=True)

    parser.set_defaults(func=lambda a: parser.print_help())
    sub_parser = parser.add_subparsers()

    s_scheduler = sub_parser.add_parser(
        'scheduler', parents=[parent], help="start scheduler")
    s_scheduler.set_defaults(func=run_scheduler)
    s_scheduler.add_argument('-n', '--max-cache-server', default=2)

    server = sub_parser.add_parser(
        "server", parents=[parent], help="start remote server")
    server.set_defaults(func=run_server)
    server.add_argument('-n', '--max-connection', default=1)
    server.add_argument('-b', '--bind', default='127.0.0.1')
    server.add_argument('-p', '--listen', default=9700)
    server.add_argument('-t', '--timeout', default=6000)

    show = sub_parser.add_parser('show-config', help='show default config')
    show.set_defaults(func=run_show)

    add_parent = argparse.ArgumentParser(add_help=False)
    add_parent.add_argument('-H', '--client-host')
    add_parent.add_argument('-P', '--client-port', default=3306)
    add_parent.add_argument('-D', '--database')
    add_parent.add_argument('-n', '--name', required=True)

    add_url = sub_parser.add_parser(
        'url', help='add url', parents=[add_parent])
    add_url.set_defaults(func=run_url)
    add_url.add_argument('-g', '--group', required=True)

    add_account = sub_parser.add_parser(
        'account', help='add account', parents=[add_parent])
    add_account.add_argument('-p', '--password', required=True)
    add_account.add_argument('-g', '--group', required=True)
    add_account.set_defaults(func=run_account)

    add_host = sub_parser.add_parser(
        'host', help='add host', parents=[add_parent])
    add_host.add_argument('-p', '--port', required=True)
    add_host.set_defaults(func=run_host)

    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    try:
        args.func(args.__dict__)
    except AttributeError:
        parser.print_help()
    except Exception as e:
        print("ERROR:\n{}\n".format(e))
        parser.print_help()


if __name__ == "__main__":
    main()
