from spider import *
from scheduler import *
import multiprocessing
from selenium import webdriver
import queue
import hashlib
import json
import logging

logging.basicConfig(level=logging.INFO)


conf = load_json('conf.json') or {}




class Task(Task):

    def __init__(self, cache):
        if not isinstance(cache, Cache):
            raise TypeError("Need a Cache")
        self._cache = cache

    def re_task(self, task_meta):
        self._cache.push(task_meta.get_kwargs()['url'])

    def has_task(self):
        return self._cache.size() > 0

    def get_task(self):
        return TaskInfo("LinkedinService", "crawl", url=self._cache.pop()['url'], debug=False)


if __name__ == "__main__":
    
    pass

    # data = json.dumps(conf, ensure_ascii=False)
    # scheduler = remote_scheduler()
    # server = conf['server']
    # elastic = conf['elasticSearch']
    # cache = ElasticCache('linkedin_cache', 'url', elastic=elastic)
    # cache.push({
    #     'url': "https://www.linkedin.com/in/theahmadimam/"
    # })
    # for s in server:
    #     scheduler.register(s['host'], s['port'])
    # scheduler.add_task(Task(cache=cache))
    # scheduler.execute_task()
    # cache.reset_data()
    