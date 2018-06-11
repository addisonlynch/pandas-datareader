import os

from pandas_datareader.base import _BaseReader

BARCHART_BASE_URL = "https://marketdata.websol.barchart.com/{}.json"


class Barchart(_BaseReader):
    """
    Base class for all Barchart queries
    """
    _format = 'json'

    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.001, session=None, api_key=None):
        super(Barchart, self).__init__(symbols=symbols, start=start,
                                       end=end, retry_count=retry_count,
                                       pause=pause, session=session)
        if api_key is None:
            api_key = os.getenv('BARCHART_API_KEY')
        if not api_key or not isinstance(api_key, str):
            raise ValueError('The Barchart API key must be provided '
                             'either through the api_key variable or '
                             'through the environment varaible '
                             'BARCHART_API_KEY')
        self.api_key = api_key

    @property
    def endpoint(self):
        raise NotImplementedError

    @property
    def url(self):
        return BARCHART_BASE_URL.format(self.endpoint)

    @property
    def params(self):
        return {
            'apikey': self.api_key,
            'symbols': ','.join(self.symbols)
        }
