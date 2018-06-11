import pandas as pd

from pandas_datareader.barchart import Barchart


class BarchartQuotesReader(Barchart):
    """
    Returns DataFrame of Barchart Stock quotes for a symbol or
    list of symbols.

    Parameters
    ----------
    symbols : string, array-like object (list, tuple, Series), or DataFrame
        Single stock symbol (ticker), array-like object of symbols or
        DataFrame with index containing stock symbols.
    retry_count : int, default 3
        Number of times to retry query request.
    pause : int, default 0.5
        Time, in seconds, to pause between consecutive queries of chunks. If
        single value given for symbol, represents the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used
    """
    def __init__(self, symbols=None, retry_count=3, pause=0.5, session=None,
                 api_key=None):
        if isinstance(symbols, str):
            syms = [symbols]
        elif isinstance(symbols, list):
            if len(symbols) > 25:
                raise ValueError("Up to 25 symbols at once are allowed.")
            else:
                syms = symbols
        super(BarchartQuotesReader, self).__init__(symbols=syms,
                                                   start=None, end=None,
                                                   retry_count=retry_count,
                                                   pause=pause,
                                                   session=session,
                                                   api_key=api_key)

    @property
    def endpoint(self):
        return "getQuote"

    def _read_lines(self, out):
        quotes = {sym["symbol"]: [sym["lastPrice"], sym["volume"],
                  sym["tradeTimestamp"]] for sym in out["results"]}
        df = pd.DataFrame.from_dict(quotes, orient='index')
        df.columns = ["price", "volume", "timestamp"]
        return df
