from .spider import *


class Twitter(Spider):

    def __init__(self, storage: Storage = None):
        super().__init__("twitter", storage)

    def start(self):
        pass
