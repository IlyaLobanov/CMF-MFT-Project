from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd
import numpy as np
from sortedcontainers import SortedDict

@dataclass
class Order:  # Our own placed order
    place_ts: float  # ts when we place the order
    exchange_ts: float  # ts when exchange(simulator) get the order
    order_id: int
    side: str
    size: float
    price: float


@dataclass
class CancelOrder:
    exchange_ts: float
    id_to_delete: int

@dataclass
class AnonTrade:  # Market trade
    exchange_ts: float
    receive_ts: float
    side: str
    size: float
    price: float


@dataclass
class OwnTrade:  # Execution of own placed order
    place_ts: float  # ts when we call place_order method, for debugging
    exchange_ts: float
    receive_ts: float
    trade_id: int
    side: str
    size: float
    price: float
    execute: str  # BOOK or TRADE

@dataclass
class OrderbookSnapshotUpdate:  # Orderbook tick snapshot
    exchange_ts: float
    receive_ts: float
    asks: List[Tuple[float, float]]  # tuple[price, size]
    bids: List[Tuple[float, float]]


@dataclass
class MdUpdate:  # Data of a tick
    exchange_ts: float
    receive_ts: float
    orderbook: Optional[OrderbookSnapshotUpdate] = None
    trade: Optional[AnonTrade] = None


def update_best_positions(best_bid: float, best_ask: float, ask_quantity: float, bid_quantity: float,
                          md: MdUpdate) -> Tuple[float, float, float, float]:
    """
    Update best ask and bid prices with market data update

    :param bid_quantity:
    :param ask_quantity:
    :param best_bid:    Best bid
    :param best_ask:    Best ask
    :param md:          Market data
    :return:            New best_bid and best_ask
    """
    if md.orderbook is not None:
        best_bid = md.orderbook.bids[0][0]
        best_ask = md.orderbook.asks[0][0]
        ask_quantity = md.orderbook.asks[0][1]
        bid_quantity = md.orderbook.bids[0][1]
    elif md.trade is not None:
        if md.trade.side == 'BID':
            best_ask = max(md.trade.price, best_ask)
        elif md.trade.side == 'ASK':
            best_bid = min(md.trade.price, best_bid)
        else:
            assert False, "WRONG TRADE SIDE"
    return best_bid, best_ask, ask_quantity, bid_quantity

def trade_to_dataframe(trades_list: List[OwnTrade]) -> pd.DataFrame:
    exchange_ts = [trade.exchange_ts for trade in trades_list]
    receive_ts = [trade.receive_ts for trade in trades_list]
    
    size = [trade.size for trade in trades_list]
    price = [trade.price for trade in trades_list]
    side = [trade.side for trade in trades_list]
    
    dct = {
        "exchange_ts": exchange_ts,
        "receive_ts": receive_ts,
        "size": size,
        "price": price,
        "side": side
    }

    df = pd.DataFrame(dct).groupby('receive_ts').agg(lambda x: x.iloc[-1]).reset_index()    
    return df


def md_to_dataframe(md_list: List[MdUpdate]) -> pd.DataFrame:
    
    best_bid = -np.inf
    best_ask = np.inf
    best_bids = []
    best_asks = []
    for md in md_list:
        best_bid, best_ask = update_best_positions(best_bid, best_ask, md)
        
        best_bids.append(best_bid)
        best_asks.append(best_ask)
        
    exchange_ts = [md.exchange_ts for md in md_list]
    receive_ts = [md.receive_ts for md in md_list]
    dct = {
        "exchange_ts": exchange_ts,
        "receive_ts": receive_ts,
        "bid_price": best_bids,
        "ask_price": best_asks
    }
    
    df = pd.DataFrame(dct).groupby('receive_ts').agg(lambda x: x.iloc[-1]).reset_index()    
    return df

class PriorQueue:
    def __init__(self):
        self._queue = SortedDict()
        self._min_key = np.inf

    def push(self, key, val):
        if key not in self._queue:
            self._queue[key] = []
        self._queue[key].append(val)
        self._min_key = min(self._min_key, key)

    def pop(self):
        if len(self._queue) == 0:
            return np.inf, None
        res = self._queue.popitem(0)
        self._min_key = np.inf
        if len(self._queue):
            self._min_key = self._queue.peekitem(0)[0]
        return res

    def min_key(self):
        return self._min_key



