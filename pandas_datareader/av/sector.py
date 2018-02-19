import pandas as pd

from pandas_datareader.av import AlphaVantage
from pandas_datareader._utils import RemoteDataError

class AVSectorPerformanceReader(AlphaVantage):

    @property
    def params(self):
        return {
            'function': self.function,
            'apikey': self.api_key
        }

    @property
    def function(self):
        return 'SECTOR'

    def _read_lines(self, out):
        if "Information" in out:
            raise RemoteDataError()
        else:
            out.pop("Meta Data")
        df = pd.DataFrame(out)
        columns = ["RT", "1D", "5D", "1M", "3M", "YTD", "1Y", "3Y", "5Y", 
                   "10Y"]
        df.columns = columns
        return df
