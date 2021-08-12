"""
Helper Functions for LIDO stETH-ETH analysis
Goal: An analysis of the most traded intervals of stETH/ETH pair by volume and by time
"""

import pandas as pd
import matplotlib.pyplot as plt
import curve_pool

def sort_utc_session(hour):
    '''
    Group hours into time sessions US, Asia, and Europe
    12-20 UTC is US
    20-4 UTC is Asia
    4-12 UTC is Europ

    Parameters
    ----------
    hour : int

    Returns
    -------
    str
    '''
    if hour in [12,13,14,15,16,17,18,19]:
        return 'US'
    elif hour in [20,21,22,23,0,1,2,3]:
        return 'Asia'
    else:
        return 'Europe'


def plot_bar_with_annotation(groupby_data,title,ylabel,xlabel,figsize = (6, 5)):
    '''
    Generate bar plot from groupby data
    Mainly used to examine volume ranges

    Parameters
    ----------
    groupby_data : groupby
    title : str
    ylabel : str
    xlabel : str
    figsize : tuple (x,y)
        The default is (6, 5).
    Returns
    -------
    '''
    groupby_data_percent = (groupby_data / groupby_data.sum())*100
    ax = groupby_data.plot(kind='bar', title=title, ylabel=ylabel,xlabel=xlabel, figsize=figsize)
    for i,p in enumerate(ax.patches):
        width = p.get_width()
        height = p.get_height()
        x, y = p.get_xy() 
        percentage_mark = groupby_data_percent.iloc[i].round(3)
        if percentage_mark == 0:
            continue
        if percentage_mark>.05:
            ax.annotate(str(percentage_mark)+'%', (x + width/2, y + height*1.02), ha='center')
        elif percentage_mark<.01:
            ax.annotate(str(percentage_mark)+'%', (x + width/2, y + height*1.5), ha='center')
        else:
            ax.annotate(str(percentage_mark)+'%', (x + width/2, y + height*1.25), ha='center')

def get_dx_dy_pool(pool,dx_list,col_names = ['ETH','stETH']):
    '''
    Given a curve pool with certain parameters, what is each traded price at initiation?

    Parameters
    ----------
    pool : Curve pool object
    dx_list : list
        list of potential trade sizes
    col_names : list, optional
       The default is ['ETH','stETH'].

    Returns
    -------
    traded_curve : DataFrame
        Has prices at each traded amount
    '''
    dy_list = []
    for dx in dx_list: 
        dy = pool.dy(0,1,dx)
        dy_list.append(dy)
    traded_curve = pd.DataFrame([dx_list,dy_list]).T
    traded_curve.columns=col_names
    traded_curve['price'] = traded_curve[col_names[0]]/traded_curve[col_names[1]]
    return traded_curve

