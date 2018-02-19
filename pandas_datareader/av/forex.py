from pandas_datareader.av import AlphaVantage

from pandas_datareader._utils import RemoteDataError

import pandas as pd


class AlphaVantageForexReader(AlphaVantage):

    def __init__(self, pairs=None, retry_count=3, pause=0.001, session=None,
                 api_key=None):

        super(AlphaVantageForexReader, self).__init__(symbols=pairs,
                                                      start=None, end=None,
                                                      retry_count=retry_count,
                                                      pause=pause,
                                                      session=session,
                                                      api_key=api_key)
        self.from_curr = {}
        self.to_curr = {}
        self.optional_params = {}
        if isinstance(pairs, str):
            self.pairs = [pairs]
        else:
            self.pairs = pairs
        try:
            for pair in self.pairs:
                self.from_curr[pair] = pair.split('/')[0]
                self.to_curr[pair] = pair.split('/')[1]
        except Exception as e:
            print(e)
            raise ValueError("Please input a currency pair "
                             "formatted 'FROM/TO' or a list of "
                             "currency pairs")

    @property
    def function(self):
        return 'CURRENCY_EXCHANGE_RATE'

    @property
    def data_key(self):
        return 'Realtime Currency Exchange Rate'

    @property
    def params(self):
        params = {
            'apikey': self.api_key,
            'function': self.function
            }
        params.update(self.optional_params)
        return params

    def read(self):
        result = []
        for pair in self.pairs:
            self.optional_params = {
                'from_currency': self.from_curr[pair],
                'to_currency': self.to_curr[pair],
                }
            data = super(AlphaVantageForexReader, self).read()
            result.append(data)
        df = pd.concat(result, axis=1)
        df.columns = self.pairs
        return df

    def _read_lines(self, out):
        try:
            df = pd.DataFrame.from_dict(out[self.data_key], orient='index')
        except KeyError:
            raise RemoteDataError()
        df.sort_index(ascending=True, inplace=True)
        df.index = [id[3:] for id in df.index]
        return df