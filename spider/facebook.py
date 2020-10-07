from .spider import Spider


class FaceBook(Spider):
    _url = "https://www.facebook.com/login/"

    def __init__(self, user=None, password=None, storage=None):
        super().__init__("facebook", storage)
        self.user = user
        self.password = password

    def start(self):

        self.save({
            "info": {
                "age": [
                    "setting"
                ]
            }
        })
