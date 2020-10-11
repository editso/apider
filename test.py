import scheduler
import logging
import time

logging.basicConfig(level=logging.INFO)


@scheduler.run_thread()
def run():
    server = scheduler.RemoteInvokeServer('0.0.0.0', 8888, None)
    server.add_service(Task())
    server.start()


class Adapter(scheduler.ConnectorAdapter):

    def get(self):
        sock = scheduler.socket.create_connection(('0.0.0.0', 8888))
        return scheduler.RemoteInvokeConnector(sock)

    def remove(self):
        pass

class Listener(scheduler.DispatchListener):

    def error(self, *args, **kwargs):
        # print("failure")
        pass

    def success(self, *args, **kwargs):
        print("success")


class Task(scheduler.Task):
    def __init__(self):
        self._queue = scheduler.Queue()
        self._queue.put("htt")
        self._queue.put("wwww")
        self._queue.put("wwww")
        self._queue.put("wwww")

    def has_task(self):
        return self._queue.qsize() > 0

    def next_task(self):
        self._queue.get()
        time.sleep(5)
        return scheduler.make_request("Task", "next_task")


run()

dispatcher = scheduler.remote_invoke_dispatcher(Adapter())
dispatcher.add_listener(Listener())
s = scheduler.Scheduler()
s.add(dispatcher)
s.register(Task())
s.register(Task())
s.dispatch()

