from sqlalchemy import Column
from sqlalchemy import *
import pymysql
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import or_
from collections import Iterable
import datetime


Base = declarative_base()


class Storage(object):
    stat = {}

    def get(self, *args, **kwargs):
        pass

    def set(self, *args, **kwargs):
        pass

    def push(self, *args, **kwargs):
        pass

    def check(self, stat):
        return stat in self.stat.values()


class DatabaseStorage(Storage):

    def __init__(self, engine, mapper_cls):
        self._engine = engine
        self._mapper_cls = mapper_cls
        Base.metadata.create_all(self._engine)
        self._session = sessionmaker(self._engine).__call__()

    @property
    def session(self):
        try:
            return self._session
        finally:
            print("...")
            self._session.expire_all()
            self._session.flush()

    @staticmethod
    def get_tuple(query):
        return tuple([query]) if not isinstance(query, Iterable) else tuple(query)

    def get(self, query, count):
        print("getter")
        return self.session.query(self._mapper_cls) \
                .filter(*DatabaseStorage.get_tuple(query)) \
                .limit(count) \
                .all()
        
    def set(self, query, update_columns, *args):
        self.session.query(self._mapper_cls).filter(
            *DatabaseStorage.get_tuple(query)
        ).update(update_columns)
        self.commit()

    def _base_query(self, query):
        if query is None:
            return ()
        if query is str or not isinstance(query, Iterable):
            query = [query]
        return (self._mapper_cls.u_stat == item for item in filter(lambda item: self.check(item), query))

    def commit(self):
        self.session.commit()


class UrlStorage(DatabaseStorage):
    stat = {
        'wait': 1,
        'success': 2,
        'failure': 3,
        'running': 4
    }

    class UrlInfo(Base):
        __tablename__ = 'url_info'
        u_group = Column(VARCHAR(255), nullable=False)
        u_target = Column(VARCHAR(255), primary_key=True, nullable=False)
        u_stat = Column(INTEGER, nullable=False, default=1, )
        u_date = Column(DATETIME, default=datetime.datetime.now)

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        def __repr__(self):
            return str({
                'group': self.u_group,
                'target': self.u_target,
                'stat': self.u_stat,
                'date': str(self.u_date)
            })

    def __init__(self, engine):
        super().__init__(engine, UrlStorage.UrlInfo)

    def get(self, group, count=1, stat=(1, 3)):
        data = super().get(query=[or_(*self._base_query(stat)),
                                  self._mapper_cls.u_group == group],
                           count=count)
        for item in data:
            self.push(group, item.u_target, 4)
        return data

    def set(self, group, query, stat=1):
        if not self.check(stat):
            return
        super().set(query=[self._mapper_cls.u_group == group,
                           or_(*self._base_query(query))],
                    update_columns={'u_stat': stat})

    def push(self, group, url, stat=1, *args, **kwargs):
        if not self.check(stat):
            return
        if not self.session.query(self._mapper_cls).get(url):
            self.session.add(self._mapper_cls(
                u_target=url, u_group=group, u_stat=stat or self.stat['wait']))
        elif self.check(stat):
            self.session.query(self._mapper_cls) \
                .filter(self._mapper_cls.u_target == url).update({
                    'u_group': group,
                    'u_stat': stat
                })
        self.commit()


class AccountStorage(DatabaseStorage):

    stat = {
        'available': 1,
        'unavailable': 2
    }

    class UserInfo(Base):
        __tablename__ = 'account_info'
        u_id = Column(VARCHAR(255), nullable=False, primary_key=True)
        u_group = Column(VARCHAR(255), nullable=False)
        u_account = Column(VARCHAR(255), nullable=False)
        u_password = Column(VARCHAR(255), nullable=False)
        u_stat = Column(Integer, nullable=False)
        u_date = Column(DATETIME, default=datetime.datetime.now)

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

    def __init__(self, engine):
        super().__init__(engine, AccountStorage.UserInfo)

    def gen_id(self, group, account):
        return "{}@{}".format(group, account)

    def get(self, group, stat=1, count=1):
        return super().get(
            query=[
                self._mapper_cls.u_group == group,
                or_(*self._base_query(stat))
            ],
            count=count
        )

    def set(self, group, query, stat):
        if not self.check(stat):
            return
        super().set(
            query=[
                self._mapper_cls.u_group == group,
                or_(*self._base_query(query))
            ],
            update_columns={
                'u_stat': stat
            }
        )

    def push(self, group, account, password, stat=1):
        u_id = self.gen_id(group, account)
        o_account = self.session.query(self._mapper_cls).get(u_id)
        if o_account and (o_account.u_password != password or o_account.u_stat != stat):
            self.session.query(self._mapper_cls)\
                .filter(self._mapper_cls.u_id == u_id)\
                .update({'u_password': password, 'u_stat': stat})
        elif not o_account:
            self.session.add(
                self._mapper_cls(
                    u_id=u_id,
                    u_group=group,
                    u_account=account,
                    u_password=password,
                    u_stat=stat)
            )
        self.commit()


class HostStorage(DatabaseStorage):

    stat = {
        'wait': 1,
        'running': 2
    }

    class HostInfo(Base):
        __tablename__ = 'host_info'
        h_id = Column(VARCHAR(255), primary_key=True)
        h_host = Column(VARCHAR(255), nullable=False)
        h_port = Column(Integer, nullable=False)
        h_ver_type = Column(Integer)
        h_stat = Column(Integer, nullable=False)

        def __repr__(self):
            return "{}:{}".format(self.h_host, self.h_port)

    def __init__(self, engine):
        super().__init__(engine, HostStorage.HostInfo)

    def _base_query(self, query):
        if query is None:
            return ()
        if query is str or not isinstance(query, Iterable):
            query = [query]
        return (self._mapper_cls.h_stat == item for item in filter(lambda item: self.check(item), query))

    def gen_id(self, host, port):
        return "{}:{}".format(host, port)

    def get(self, stat=1, count=1):
        return super().get(
            query=[
                or_(*self._base_query(stat))
            ],
            count=count
        )

    def push(self, host, port, stat=1, ver_type=None):
        if not self.check(stat):
            return
        h_id = self.gen_id(host, port)
        o_host = self.session.query(self._mapper_cls).get(h_id)
        if o_host and (o_host.h_stat != stat):
            super().set(
                query=self._mapper_cls.h_id == h_id,
                update_columns={
                    'h_stat': stat
                }
            )
        elif not o_host:
            self.session.add(
                self._mapper_cls(
                    h_id=h_id,
                    h_host=host,
                    h_port=port,
                    h_ver_type=ver_type,
                    h_stat=stat
                )
            )
        self.commit()


def make_mysql(user, password, db, host='127.0.0.1', port='3306'):
    return create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(
        user, password, host, port, db
    ))
