import os

from pandas_datareader.base import _BaseReader
from pandas_datareader._utils import RemoteDataError

import pandas as pd

AV_BASE_URL = 'https://www.alphavantage.co/query'


class AlphaVantage(_BaseReader):

    _format = 'json'

    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.001, session=None, api_key=None):
        super(AlphaVantage, self).__init__(symbols=symbols, start=start,
                                           end=end, retry_count=retry_count,
                                           pause=pause, session=session)
        if api_key is None:
            api_key = os.getenv('ALPHAVANTAGE_API_KEY')
        if not api_key or not isinstance(api_key, str):
            raise ValueError('The AlphaVantage API key must be provided '
                             'either through the api_key variable or '
                             'through the environment varaible '
                             'ALPHAVANTAGE_API_KEY')
        self.api_key = api_key

    @property
    def url(self):
        """ API URL """
        return AV_BASE_URL

    @property
    def params(self):
        return {
            'symbol': self.symbols,
            'function': self.function,
            'apikey': self.api_key
            }

    @property
    def function(self):
        raise NotImplementedError

    @property
    def data_key(self):
        raise NotImplementedError
    
    def _read_lines(self, out):
        try:
            df = pd.DataFrame.from_dict(out[self.data_key], orient='index')
        except KeyError:
            raise RemoteDataError()
        df.sort_index(ascending=True, inplace=True)
        df.columns = [id[3:] for id in df.columns]
        return df
