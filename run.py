import argparse
import storage


mapper_engine = storage.make_mysql('root', '79982473', 'test')
url_cache = storage.UrlStorage(mapper_engine)
account_cache = storage.AccountStorage(mapper_engine)
host_cache = storage.HostStorage(mapper_engine)


def account():
    account_cache.push('linkedin', 'test', '79982473', 2)
    account_cache.set('linkedin', [1,2], 3)
    print(account_cache.get('linkedin', 1))


def host():
    host_cache.push('127.0.0.1', 8080)
    print(host_cache.get())

if __name__ == '__main__':
    # account()
    host()