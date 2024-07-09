import requests

API = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo"


def get_article():
    response = requests.get(API)
    return response.text