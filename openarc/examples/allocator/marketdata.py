#!/usr/bin/env python3

import decimal
import pprint

from openarc.graph  import *
from openarc.oatime import *

from gevent import spawn

class OAG_WsbBalancer(OAG_RootNode):
    @staticproperty
    def context(cls): return "levelcompute"

    @staticproperty
    def dbindices(cls) : return {
    }

    @staticproperty
    def streams(cls): return {
        'SVXY'       : [ OAG_Ticker, None, "rebalance_portfolio" ],
        'XIVH'       : [ OAG_Ticker, None, "rebalance_portfolio" ],
        'NFLX'       : [ OAG_Ticker, None, "rebalance_portfolio" ],
        'TSLA'       : [ OAG_Ticker, None, "rebalance_portfolio" ],
        'AMD'        : [ OAG_Ticker, None, "rebalance_portfolio" ]
    }

    cash_position = decimal.Decimal(1000000)

    @property
    def ticker_positions(self):
        ticker_positions = getattr(self, '_ticker_positions', None)
        if not ticker_positions:
            self._ticker_positions = {ticker:decimal.Decimal(0.0) for ticker in self.streams}
        return self._ticker_positions

    @ticker_positions.setter
    def ticker_positions(self, value):
        self._ticker_positions = value

    @oagprop
    def portfolio_value(self):

        ticker_prices = {}

        for ticker in self.streams:
            ticker_prices[ticker] = decimal.Decimal(0)
            ticker_oag = getattr(self, ticker, None)
            if ticker_oag:
                ticker_prices[ticker] = ticker_oag.price

        return {ticker:ticker_prices[ticker]*self.ticker_positions[ticker] for ticker in ticker_prices}

    def rebalance_portfolio(self):

        order_book = {
            ticker:{
                'buy'  : decimal.Decimal(0),
                'sell' : decimal.Decimal(0)
            } for ticker in self.streams }

        ticker_positions = self.ticker_positions

        pprint.pprint('starting positions:')
        pprint.pprint(ticker_positions)
        pprint.pprint('portfolio value')
        pprint.pprint(self.portfolio_value)

        per_ticker_target = sum(list(self.portfolio_value.values()))/len(self.streams)
        print('per_ticker_target', per_ticker_target)
        if per_ticker_target>0:
            for ticker in self.streams:
                ticker_oag = getattr(self, ticker, None)
                if ticker_oag:
                    delta_value = self.portfolio_value[ticker] - per_ticker_target
                    order_book[ticker]['price'] = ticker_oag.price
                    if delta_value >= 0:
                        order_book[ticker]['sell'] = delta_value/ticker_oag.price
                        ticker_positions[ticker] -= order_book[ticker]['sell']
                    else:
                        order_book[ticker]['buy'] = -delta_value/ticker_oag.price
                        ticker_positions[ticker] += order_book[ticker]['buy']

        # Liquidate cash position
        per_ticker_cash = self.cash_position/len(self.streams)
        if per_ticker_cash>0:
            print("Liquidating cash position")
            for ticker in self.streams:
                ticker_oag = getattr(self, ticker, None)
                if ticker_oag:
                    order_book[ticker]['price'] = ticker_oag.price
                    order_book[ticker]['buy'] += per_ticker_cash/ticker_oag.price
                    ticker_positions[ticker] += order_book[ticker]['buy']

        pprint.pprint('order book')
        pprint.pprint(order_book)
        pprint.pprint('ending positions')
        pprint.pprint(ticker_positions)
        print("=========>")

        self.cash_position = 0
        self.ticker_positions = ticker_positions

class OAG_Ticker(OAG_RootNode):
    @staticproperty
    def context(cls): return "levelcompute"

    @staticproperty
    def dbindices(cls) : return {
        'ticker' : [ ['ticker'], False  ,      None  ]
    }

    @staticproperty
    def streams(cls): return {
        'ticker'     : [ 'varchar(50)', str,  None ],
        'price'      : [ 'decimal',     None, None ],
        'updatetime' : [ 'timestamp',   None, None ],
    }

    def __cb_price_monitor(self):

        from bs4 import BeautifulSoup
        import requests

        while True:

            # Some boilerplate to retrieve prices from yahoo finance. This
            # could just as easily be replaced by a random number generator.
            quote = requests.get('https://finance.yahoo.com/quote/%s' % self.ticker)
            soup = BeautifulSoup(quote.text.encode('utf-8'), "html.parser")
            current_time = OATime().now
            new_price = decimal.Decimal([span for span in soup.find_all('span') if span['data-reactid']=='14'][0].contents[1].replace(',', ''))

            if new_price != self.price:
                print("===> Price update detected for [%s], updating database from [%s] to [%s]" % (self.ticker, self.price, new_price))

                # Updating properties in a transaction.
                #
                # Typically, setting a property will trigger an immediate notification of
                # node invalidation to upstream consumers. By using an rpc.transaction,
                # invalidation notifications are held until the context manager dies.
                with self.rpc.transaction:
                    self.updatetime = current_time
                    self.price = new_price
                self.db.update()

            # Status report
            print('[%s] [%s] %s' % (current_time, self.ticker, self.price))

            # Next price check in 15 seconds
            gevent.sleep(15)

    def start_price_monitor(self):
        self.rpc._glets.append(spawn(self.__cb_price_monitor))
        return self.rpc._glets
