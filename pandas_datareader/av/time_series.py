from pandas_datareader.av import AlphaVantage

import pandas as pd 
from datetime import datetime


class AVTimeSeriesReader(AlphaVantage):

    _FUNC_TO_DATA_KEY={
    "TIME_SERIES_DAILY": "Time Series (Daily)",
    "TIME_SERIES_DAILY_ADJUSTED": "Time Series (Daily)",
    "TIME_SERIES_WEEKLY": "Weekly Time Series",
    "TIME_SERIES_WEEKLY_ADJUSTED": "Weekly Adjusted Time Series",
    "TIME_SERIES_MONTHLY": "Monthly Time Series",
    "TIME_SERIES_MONTHLY_ADJUSTED": "Monthly Adjusted Time Series"
    }

    def __init__(self, symbols=None, function="TIME_SERIES_DAILY", 
                 start=None, end=None, retry_count=3, pause=0.35, 
                 session=None, chunksize=25, api_key=None):
        super(AVTimeSeriesReader, self).__init__(symbols=symbols, start=start,
                                             end=end, retry_count=retry_count,
                                             pause=pause, session=session,
                                             api_key=api_key)

        self.func = function

    @property
    def function(self):
        return self.func

    @property
    def output_size(self):
        delta = datetime.now() - self.start
        return 'full' if delta.days > 80 else 'compact'

    @property
    def data_key(self):
        return self._FUNC_TO_DATA_KEY[self.function]

    @property
    def params(self):
        return{
        "symbol": self.symbols,
        "function": self.function,
        "apikey" : self.api_key,
        "outputsize": self.output_size
        }

    def _read_lines(self, out):
        data = super(AVTimeSeriesReader, self)._read_lines(out)
        start_str = self.start.strftime('%Y-%m-%d')
        end_str = self.end.strftime('%Y-%m-%d')
        data = data.loc[start_str:end_str]
        if data.empty:
            raise ValueError("Please input a valid date range")
        else:
            for column in data.columns:
                if column == 'volume':
                    data[column] = data[column].astype('int64')
                else:
                    data[column] = data[column].astype('float64')
        return data
