from scheduler.dynamic import load_module
import argparse
import scheduler
import spider
import json
import multiprocessing
import logging
import storage
import sys
import getpass
import os
from storage import HostStorage
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

__url_cache_name__ = 'linkedin_cache2'


__tmp_config_name__ = '.config.json'


def get_elasticsearch():
    es = scheduler.__config__['es']
    return {
        'hosts': es['host'],
        'ports': es['port'],
        'scheme': es['scheme'],
        'user': es['user'],
        'password': es['password']
    }


def load_config(config, ignore=True):
    try:
        with open(config, 'r', encoding='utf-8') as c:
            __base_config__.update(json.loads(c.read()))
    except Exception as e:
        if not ignore:
            raise e
    if sys.platform == 'win32':
        try:
            with open(__tmp_config_name__, 'w+', encoding='utf8') as f:
                f.write(json.dumps(__base_config__, ensure_ascii=False))
        except Exception as e:
            pass
    scheduler.__config__ = __base_config__


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


def set_work(args):
    dir = args['work_dir']
    if not dir:
        return
    os.chdir(dir)

def run_scheduler(args):
    set_work(args)
    load_config(args['config'], ignore=False)
    mysql_engine = storage.make_mysql(**__base_config__['mysql'])
    ts = scheduler.Scheduler()
    modelue = scheduler.load_module(args['module'])
    tasks = scheduler.new_all_instalce(modelue, (scheduler.Task, scheduler.ConnectorAdapter))
    dispatcher = scheduler.remote_invoke_dispatcher(ConnectAdapter(cache=storage.HostStorage(mysql_engine)))
    for o in tasks:
        if isinstance(o, scheduler.Task):
            ts.register(o)
        if isinstance(o, scheduler.DispatchListener):
            dispatcher.add_listener(o)
    ts.add_dispatcher(dispatcher)
    ts.dispatch()


def run_server(args):
    set_work(args)
    load_config(args['config'], ignore=False)
    if __name__ != "__main__":
        return
    server = __base_config__['server']
    server.update(args)
    server = scheduler.RemoteInvokeServer(
        host=server['bind'],
        port=int(server['listen']),
        invoke_timeout=server['timeout'],
        max_connection=server['max_connection'])
    module = scheduler.load_module(args['module'])
    services = scheduler.find_class(module, scheduler.RemoteService)
    listeners = scheduler.new_all_instalce(module, scheduler.RemoteInvokeListener)
    for o in listeners:
        server.add_listener(o)
    for service in services:
        server.add_service(service)
    server.start()


def make_client(args):
    
    return storage.make_mysql(input('Enter user: '),
                              getpass.getpass("Enter password: "),
                              args['database'] or input('Enter database: '),
                              args['client_host'] or input('Enter host: '),
                              args['client_port'])


def run_url(args):
    load_config(args['config'])
    cache = spider.ElasticCache(
        __url_cache_name__, elastic=get_elasticsearch())
    cache.push({'url': args['name']})


def run_account(args):
    load_config(args['config'])
    cache = spider.LinkedAccount(**get_elasticsearch())
    cache.add(args['name'], args['password'], ignore_ivalid=True)


def run_host(args):
    load_config(args['config'])
    __base_config__.update(args)
    mysql = storage.make_mysql(**__base_config__['mysql'] or make_client())
    cache = HostStorage(mysql)
    cache.push(args['name'], args['port'])


def run_show(args):
    print(json.dumps(__base_config__))


def parse_args():
    parser = argparse.ArgumentParser(sys.argv[0].split('.')[0])
    parent = argparse.ArgumentParser(add_help=False)

    parent.add_argument('-D', '--deamon', default=False)
    parent.add_argument('-d', '--debug', default=False, type=bool)
    parent.add_argument('-c', '--config', required=True)
    parent.add_argument('-w', '--work-dir')
    parent.add_argument('-m', '--module',required=True, help='run module')

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
    add_parent.add_argument('-c', '--config')

    add_url = sub_parser.add_parser(
        'url', help='add url', parents=[add_parent])
    add_url.set_defaults(func=run_url)
    add_url.add_argument('-g', '--group')

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
    if 'debug' in args and args.debug:
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
    if sys.platform == 'win32':
        load_config(__tmp_config_name__)
    multiprocessing.freeze_support()
    parse_args()
