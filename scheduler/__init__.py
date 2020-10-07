from .scheduler import *
from .handler import *
from .server import *
from .client import *


def remote_scheduler():
    with RemoteScheduler(JsonDeCoder(), JsonEnCoder()) as remote:
        return remote


def create_remote_server(port, handler, host='0.0.0.0'):
    with RemoteServer(port=port, host=host, handler=handler) as remote:
        remote.start()
        return remote
