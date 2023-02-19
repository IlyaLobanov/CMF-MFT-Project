from backtesting import Strategy, Backtest
import pandas as pd
import numpy as np

from data_classes import OwnTrade, MdUpdate, update_best_positions
from typing import List, Optional, Tuple, Union, Deque, Dict

from data_classes import Order
from simulator import Simulator


class Backtest: 
    '''
    Backtester of trading strategies 
    '''
    def __init__(self, pred: np.array, T: float, M: float, C: float, S: float): 
        
        '''
        Params: 
        market_price: pd.DataFrame of ask and bid prices 
        pred: pd.DataFrame with model predictions for each Timestamp 

        T: threshold for trades 
        M: risk-managemt, maximum pos we can hold 
        C: commission for each trade 
        S: size of each trade 
        '''
        
        self.T = T
        self.M = M
        self.S = S
        self.C = C
        self.predictions = pred 


        self.asset_position = 0

        self.buy_prices = []  # price history of buy trades
        self.sell_prices = []  # price history of sell trades

        self.buy_volumes = []  # volume history of buy trades
        self.sell_volumes = []  # volume history of sell trades

        self.commissions = []  # commission history of trades
        self.inventory = []  # history of our inventory

        self.cash = 10e7  # cash balance
        self.latency = 0

        self.trade_id = 0  # id of the last trade
        self.order_id = 0  # id of our order 

        # pd.DataFrame with results of trades
        self.result = pd.DataFrame(columns = ['place_ts', 'exchange_ts', 'recieve_ts', 'ID', 'is_buy', 'Volume', 'Price', 'Type'])  

    def save_trade(self, trade: OwnTrade): 
        '''
        Save trade into the result DataFrame
        '''
        self.result = self.result.append({
                                'place_ts': trade.place_ts,
                                'exchange_ts': trade.exchange_ts,
                                'recieve_ts': trade.receive_ts,
                                'ID': trade.trade_id,
                                'is_buy': trade.side,
                                'Volume': trade.size,
                                'Price': trade.price,
                                'Type': trade.execute},
                                ignore_index=True)
        self.order_id += 1


    def trade(self, sim: Simulator) -> \
            Tuple[List[OwnTrade], List[MdUpdate], List[Union[OwnTrade, MdUpdate]], List[Order]]:
        """
            This function runs simulation

            Args:
                sim(Sim): simulator
            Returns:
                trades_list(List[OwnTrade]): list of our executed trades
                md_list(List[MdUpdate]): list of market data received by strategy
                updates_list( List[ Union[OwnTrade, MdUpdate] ] ): list of all updates
                received by strategy(market data and information about executed trades)
                all_orders(List[Orted]): list of all placed orders
        """
        # market data list
        md_list: List[MdUpdate] = []
        # executed trades list
        trades_list: List[OwnTrade] = []
        # all updates list
        updates_list = []
        # current best positions
        best_bid = -np.inf
        best_ask = np.inf

        quantity_ask = 0
        quantity_bid = 0

        i = 0
        # last order timestamp
        prev_time = -np.inf
        # orders that have not been executed/canceled yet
        ongoing_orders: Dict[int, Order] = {}
        all_orders = []
        while True:
            # get update from simulator
        

            receive_ts, updates = sim.tick()    
            if updates is None:
                break
            # save updates
            updates_list += updates

            for update in updates:
                # update best position


                if isinstance(update, MdUpdate):
                    if update.orderbook is not None:
                        best_bid, best_ask, quantity_ask, quantity_bid  = update_best_positions(best_bid, best_ask, quantity_ask, quantity_bid, update)
                        self.midprice = (best_ask + best_bid) / 2
                        md_list.append(update)

                        if self.predictions[i] - best_ask > self.T:
                            if self.asset_position < self.M:
                                trade_buy = OwnTrade(place_ts=update.exchange_ts,
                                                    exchange_ts=update.exchange_ts,
                                                    receive_ts=update.receive_ts,
                                                    trade_id=self.order_id,
                                                    side='ASK',
                                                    size= self.S,
                                                    price=best_ask,
                                                    execute='TRADE'
                                                    )
                                self.asset_position += self.S
                                self.cash -= self.S * self.midprice * (1 + self.C)
                            
                                trades_list.append(trade_buy)
                                updates_list.append(trade_buy)
                                all_orders += [trade_buy]
                                
                                self.save_trade(trade_buy)

                        elif best_bid - self.predictions[i] > self.T: 
                            if self.asset_position> self.M: 
                                trade_sell = OwnTrade(place_ts=update.exchange_ts,
                                                exchange_ts=update.exchange_ts,
                                                receive_ts=update.receive_ts,
                                                trade_id=self.order_id,
                                                side='BID',
                                                size= self.S,
                                                price=best_bid,
                                                execute='TRADE'
                                                )
                                self.asset_position += self.S
                                self.cash += self.S * self.midprice * (1 - self.C)

                                trades_list.append(trade_sell)
                                updates_list.append(trade_sell)
                                all_orders += [trade_sell]

                                self.save_trade(trade_sell)
                        i += 1
                
                    
                elif isinstance(update, OwnTrade):
                    continue
                else:
                    assert False, 'Invalid type'


                # place order


        return trades_list, md_list, updates_list, all_orders, self.result


            




        

