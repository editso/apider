import requests
from os.path import join


class Register(object):


    def __init__(self):
        pass


    def getNumber(self):
        pass


class GetsmsRegister(Register):

    def __init__(self):
        super().__init__()
        self._api_key = 'R6Z52Z6YSG7V2VYTIRCCIH2S6K0XGUVM'
        self._base_url = 'http://api.getsms.online/'
        """
        http://api.getsms.online/stubs/handler_api.php?api_key=R6Z52Z6YSG7V2VYTIRCCIH2S6K0XGUVM&action=getNumber&service=fb&country=ru
        """

    def getNumber(self):
        return requests.post('{}stubs/handler_api.php'.format(self._base_url), data={
            'api_key': self._api_key,
            'action': 'getNumber',
            'service': 'fb',
            'country': 'ru'
        })