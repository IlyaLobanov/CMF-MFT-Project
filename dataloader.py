from typing import List

import pandas as pd

from simulator import MdUpdate
from data_classes import AnonTrade, OrderbookSnapshotUpdate


def load_before_time(path):
    df = pd.read_csv(path)
     
    return df


def load_trades(path: str) -> List[AnonTrade]:
    """
    This function downloads trades data

    :param path:    Path to file
    :param t:       Max timestamp from the first one in nanoseconds
    :return:        List of market trades
    """

    trades = load_before_time(path + 'trades.csv')
    
    # переставляю колонки, чтобы удобнее подавать их в конструктор AnonTrade
    trades = trades[['exchange_ts', 'receive_ts', 'aggro_side', 'size', 'price']].\
        sort_values(["exchange_ts", 'receive_ts'])

    # receive_ts = trades.receive_ts.values
    # exchange_ts = trades.exchange_ts.values
    trades = [AnonTrade(*args) for args in trades.values]
    return trades


def load_books(path: str) -> List[OrderbookSnapshotUpdate]:
    """
    This function downloads orderbook market data

    :param path:    Path to file
    :param t:       Max timestamp from the first one in nanoseconds
    :return:        List of orderbooks
    """
    lobs = load_before_time(path + 'lobs.csv')
    

    
    # timestamps
    receive_ts = lobs.receive_ts.values
    exchange_ts = lobs.exchange_ts.values 
    # список ask_price, ask_vol для разных уровней стакана
    # размеры: len(asks) = 10, len(asks[0]) = len(lobs)
    asks = [list(zip(lobs[f"ask_price_{i}"], lobs[f"ask_vol_{i}"])) for i in range(5)]
    # транспонируем список
    asks = [[asks[i][j] for i in range(len(asks))] for j in range(len(asks[0]))]
    # то же самое с бидами
    bids = [list(zip(lobs[f"bid_price_{i}"], lobs[f"bid_vol_{i}"])) for i in range(5)]
    bids = [[bids[i][j] for i in range(len(bids))] for j in range(len(bids[0]))]
    
    books = list(OrderbookSnapshotUpdate(*args) for args in zip(exchange_ts, receive_ts, asks, bids))
    return books


def merge_books_and_trades(books: List[OrderbookSnapshotUpdate], trades: List[AnonTrade]) -> List[MdUpdate]:
    """
    This function merges lists of orderbook snapshots and trades

    :param books:       List of orderbook snapshots
    :param trades:      List of market trades
    :return:            Merged (by time) list of MdUpdates
    """
    trades_dict = {(trade.exchange_ts, trade.receive_ts): trade for trade in trades}
    books_dict = {(book.exchange_ts, book.receive_ts): book for book in books}
    
    ts = sorted(trades_dict.keys() | books_dict.keys())

    md = [MdUpdate(*key, books_dict.get(key, None), trades_dict.get(key, None)) for key in ts]
    return md


def load_md_from_file(path: str) -> List[MdUpdate]:
    """
    This function downloads orderbooks and trades and merges them

    :param path:        Path to download from
    :param run_time:           Max timestamp from the first one in nanoseconds
    :return:            Merged (by time) list of MdUpdates
    """
    books = load_books(path)
    trades = load_trades(path)
    return merge_books_and_trades(books, trades)