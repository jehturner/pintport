import sqlite3
from collections.abc import MutableSequence, Iterable

import pandas_datareader as pdr

from . import config


class Source:
    """
    A class describing and abstracting a source of historical pricing
    information for a security.
    """
    def __init__(self, name, source, query, exchange, symbol=None,
                 currency='USD'):
        self.ID = None
        self.name = name  # duplicates the Asset / source table name?
        self.source = source
        self.query = query
        self.exchange = exchange
        self.symbol = symbol
        self.currency = currency

        try:
            self.api_key = config['api_keys'][self.source]
        except KeyError:
            self.api_key = None

    def __repr__(self):
        return("<{0}(name='{name}', source='{source}', query='{query}'"
               "exchange='{exchange}', symbol='{symbol}', "
               "currency='{currency}') ID={ID}>".format(
            self.__class__.__name__,
            name=self.name,
            source=self.source,
            query=self.query,
            exchange=self.exchange,
            symbol=self.symbol,
            currency=self.currency,
            ID=self.ID
        ))

    def __call__(self, start=None, end=None):
        """
        Look up time series from the defined source.

        Parameters
        ----------

        start : string, int, date, datetime, Timestamp
            left boundary for range (defaults to 1/1/2010)
        end : string, int, date, datetime, Timestamp
            right boundary for range (defaults to today)

        Returns
        -------

        `pandas.core.frame.DataFrame`
            Pandas DataFrame containing the time series.

        """

        return pdr.data.DataReader(name=self.symbol, data_source=self.query,
                                   start=start, end=end, session=None,
                                   api_key=self.api_key)


class SourceList(MutableSequence):
    """
    A priority-ordered list of Source instances for a given security, with
    unique identifiers, that can be mapped to a database table and interrogated
    to provide data for a given security.

    This currently allows the same Source to be added to the list more than
    once, altering the ID each time, and could do with re-thinking a bit, but
    since it's otherwise a working list object I'll check it in for future
    reference before restructuring things slightly.
    """
    def __init__(self, sources=None):
        super().__init__()
        self._list = self._check_items(sources, to_list=True)

    def __getitem__(self, index):
        return self._list[index]

    def __setitem__(self, index, value):
        self._list[index] = self._check_items(value, to_list=False)

    def __delitem__(self, index):
        del self._list[index]

    def __len__(self):
        return len(self._list)

    def insert(self, index, value):
        self._list.insert(index, self._check_item(value))

    # inherited append works automatically

    def _check_item(self, item, set_ID=True):
        if not isinstance(item, Source):
            raise ValueError('{} items must be Source instances'
                             .format(self.__class__.__name__))
        if set_ID is True:
            item.ID = self._next_ID()
        elif set_ID is not False:
            item.ID = set_ID
        return item

    def _check_items(self, items, to_list=False):
        if items is None:
            return []
        if isinstance(items, Iterable):
            for n, item in enumerate(items, start=self._next_ID()):
                self._check_item(item, set_ID=n)
        elif to_list:
            items = [self._check_item(items)]
        else:
            items = self._check_item(items)
        return items

    def _next_ID(self):
        """
        Return the next available new indentifier. These should not be re-used
        because they may be recorded in an Asset's time series (nor should the
        Source be deleted if that is the case), but it would be very
        inefficient to store a UUID in every row of a time series, so just
        return one more than the highest number in use (normally len(list)),
        to minimize the likelihood of re-using any deleted identifiers.
        """
        if not hasattr(self,  '_list'):
            return 0
        return max((-1 if source.ID is None else source.ID for source in
                    self._list), default=-1) + 1

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self._list))

