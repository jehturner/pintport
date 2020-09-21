from collections.abc import MutableSequence, Iterable
from copy import copy
import sqlite3

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


class _SourceList(MutableSequence):
    """
    A priority-ordered list of Source instances for a given security, with
    unique identifiers, that can be mapped to a database table and interrogated
    to provide data for a given security.

    This private class is intended to be used by Asset. Its ID numbers need
    to remain synchronized with those recorded in the asset price data.
    """
    def __init__(self, sources=None):
        super().__init__()
        sources = [] if sources is None else copy(sources)
        self._list = self._check_items(sources)

    def __getitem__(self, index):
        return self._list[index]

    def __setitem__(self, index, value):
        _list = self._list.copy()
        _list[index] = value
        self._list = self._check_items(_list)

    def __delitem__(self, index):
        del self._list[index]

    def __len__(self):
        return len(self._list)

    def insert(self, index, value):
        _list = self._list.copy()
        _list.insert(index, value)
        self._list = self._check_items(_list)

    # inherited append works automatically

    def _check_items(self, _list):

        ID = self._next_ID()
        names, IDs = [], []

        # Check input Sources before setting their IDs so as not to change
        # them if the operation fails:
        for item in _list:
            if not isinstance(item, Source):
                raise ValueError('items must be Source instances')
            if item.name in names:
                raise ValueError('duplicate item {}'.format(item.name))
            names.append(item.name)
            if item.ID is not None:
                if item.ID in IDs:
                    raise ValueError('duplicate ID {}'.format(item.ID))
                IDs.append(item.ID)
        for item in _list:
            if item.ID is None:
                item.ID = ID
                ID += 1

        return _list

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

