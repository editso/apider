from scheduler import *

from spider import *

import logging


logging.basicConfig(level=logging.INFO)
conf = load_json('server.json')


if __name__ == "__main__":
    listen = conf['listen']
    decoder = JsonDeCoder()
    encoder = JsonEnCoder()
    handler = RemoteClientHandler(decoder=decoder, encoder=encoder)  
    for port in listen:
        Server('0.0.0.0', port, handler=handler).start()
