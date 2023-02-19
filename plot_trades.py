import seaborn as sns
import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt 

from trade_statistics import TradeStats

class Graphics:
    '''
    Plot different statistics of trading process
    '''
    

    def __init__(self, md):
        
        self.data = md
        self.stats = TradeStats()
        self.pnl = self.stats.get_pnl(md)
    
    def trades_price(self):
        pass
    
    def market_price(self):
        pass

    def predicted_price(self):
        pass

    def cum_no_commission(self):
        pass

    def cum_PnL(self):
        plt.rcParams["figure.figsize"] = (15,10)
        plt.plot(pd.to_datetime(self.pnl['exchange_ts']) ,self.pnl['total'] ,color='blue', label="PnL")
        plt.title("PnL of the model")
        plt.ylabel("PnL value")
        plt.xlabel("Time")
        plt.legend()
        plt.show()
    
    def position(self):
        pass

    def trades_volume(self):
        pass


    def PnL_distribution(self):
        pass