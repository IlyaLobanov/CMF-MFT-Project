import numpy as np
import pandas as pd 
import math

from typing import List, Union

from data_classes import MdUpdate, OwnTrade, update_best_positions


class TradeStats:
    '''
    Collect trading statistics and metrics
    '''

    def __init__(self):
        '''
        Data is pd.DataFrame:
        Columns description 

        ID: trade_id 
        place_ts: ts when we call place_order method
        exchange_ts: ts when simulator gets the order
        Price: Price of the trade
        Volume: Volume of the trade
        is_buy: Side of the trade - ASK if buy-side, BID if sell-side  
        '''
        
        self.PnL = 0
        self.commission_history = []

    def get_pnl(self, updates_list: List[Union[MdUpdate, OwnTrade]], commission = 0.001) -> pd.DataFrame:
            
        """
        This function calculates pnl from the list of updates

        :param updates_list:    list of md updates
        :param cost:            negative commission
        :return:                pnl of updates_list (strategy's return)
        """

        # current position in btc and usd
        token_pos, usd_pos = 0.0, 0.0
        
        n = len(updates_list)
        token_pos_arr = np.zeros((n, ))
        usd_pos_arr = np.zeros((n, ))
        mid_price_arr = np.zeros((n, ))
        # current best_bid and best_ask
        self.best_bid: float = -np.inf
        self.best_ask: float = np.inf


        self.ask_quantity = 1
        self.bid_quantity = 1
        

        for i, update in enumerate(updates_list):
            
            if isinstance(update, MdUpdate):
                self.best_bid, self.best_ask, self.ask_quantity, self.bid_quantity \
                    = update_best_positions(self.best_bid, self.best_ask, self.bid_quantity, self.ask_quantity, update)
            # mid price to calculate current portfolio value
            mid_price = 0.5 * (self.best_ask + self.best_bid)
            
            if isinstance(update, OwnTrade):
                trade = update
                # update positions
                if trade.side == 'BID':
                    token_pos += trade.size
                    usd_pos -= trade.price * trade.size
                elif trade.side == 'ASK':
                    token_pos -= trade.size
                    usd_pos += trade.price * trade.size
                usd_pos -= commission * trade.price * trade.size
                self.commission_history.append(commission * trade.price * trade.size)
            

            # current portfolio value

            token_pos_arr[i] = token_pos
            usd_pos_arr[i] = usd_pos
            mid_price_arr[i] = mid_price
        
        worth_arr = token_pos_arr * mid_price_arr + usd_pos_arr
        receive_ts = [update.receive_ts for update in updates_list]
        exchange_ts = [update.exchange_ts for update in updates_list]
        
        df = pd.DataFrame({"exchange_ts": exchange_ts, "receive_ts": receive_ts, "total": worth_arr, "BTC": token_pos_arr,
                        "USD": usd_pos_arr, "mid_price": mid_price_arr})
        self.PnL = df

        return df

    def num_trades(self):
        pnl = self.get_pnl()
        return pnl.shape[0]

    def trades_volume(self):
        pnl = self.get_pnl()
        return pnl

    def sharpe_ratio(self):
        pnl = self.get_pnl()
        daily_pnl = pnl.diff().dropna()
        return daily_pnl.mean() / daily_pnl.std()

    
    def avg_PnL(self):
        pnl = self.get_pnl()

        return pnl.diff().dropna().mean()

    def avg_comission(self):
        return self.commission_history.mean()

    def share_PnL(self):
        pass


    def max_drawdown(self):
        pnl = self.get_pnl()
        Roll_Max = pnl.cummax()
        Drawdown = pnl/Roll_Max - 1
        return Drawdown.min()
    