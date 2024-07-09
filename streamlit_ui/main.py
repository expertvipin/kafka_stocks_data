import streamlit as st
import pandas as pd
import sys
import plotly.graph_objects as go
import mplfinance as mpf
sys.path.append("../consumer/")
from database import get_df_data


df = get_df_data()

df['date'] = pd.to_datetime(df['date'])

fig = go.Figure(data=[go.Candlestick(x=df['date'],
                                    open=df['open'],
                                    high=df['high'],
                                    low=df['low'],
                                    close=df['close'])])

fig.update_layout(title='Candlestick Chart',
                  xaxis_title='Date',
                  yaxis_title='Price')

st.plotly_chart(fig)


st.subheader('Data Table')
st.dataframe(df.sort_values(by='id', ascending=True), height=300, width=700)  # Sorting by Date and adjusting size
