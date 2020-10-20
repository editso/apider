import argparse
import storage
import scheduler


mapper_engine = storage.make_mysql('root', '79982473', 'test')
url_cache = storage.UrlStorage(mapper_engine)
account_cache = storage.AccountStorage(mapper_engine)
host_cache = storage.HostStorage(mapper_engine)


def account():
    account_cache.push('linkedin', 'test', '79982473', 2)
    account_cache.set('linkedin', [1,2], 3)
    print(account_cache.get('linkedin', 1))


def host():
    host_cache.push('127.0.0.1', 9999, stat=1)
    host_cache.push('127.0.0.1', 8888, stat=1)
    print(host_cache.get(stat=1, count=10))


a = 10

pipe = scheduler.multiprocessing.Pipe()
@scheduler.run_thread()
@scheduler.process_poller(interval=1)
def poller():
    pipe[0].send("hello")


@scheduler.run_thread()
@scheduler.process_poller(interval=1)
def poller2():
    print(pipe[1].recv())




if __name__ == '__main__':
    host()