import json
import os

import numpy as np
import pandas as pd
import quantstats as qs
import requests
import yahooquery as yq

from cache import file_cache

# https://www.bankofcanada.ca/rates/interest-rates/corra/
CANADA_RISK_FREE_RATE = 5.04
# 1Y rate of return for S&P/TSX Composite Index
TSX_EXPECTED_RETURN = 1.61


@file_cache('companies.json')
def get_companies():
    companies = set()
    tsx_raw = requests.get("https://www.tsx.com/json/company-directory/search/tsx/^*").json()
    for tsx in tsx_raw['results']:
        companies.add(tsx['symbol'])
        for instrument in tsx['instruments']:
            companies.add(instrument['symbol'])
    return list(companies)


def save_to_file(d, file_index):
    if not os.path.exists("output"):
        os.mkdir("output")
    with open('output/data-' + str(file_index) + '.json', 'w') as output:
        json.dump(d, output)


def get_risk(symbol):
    returns = qs.utils.download_returns(symbol)
    if returns is None or not isinstance(returns, pd.Series):
        raise ValueError('cannot find risk - ' + symbol)
    return qs.stats.var(returns), qs.stats.cvar(returns)


def get_esg(symbol, ticker):
    environment, social, governance = float('nan'), float('nan'), float('nan')
    if not isinstance(ticker.esg_scores.get(symbol), str):
        environment = ticker.esg_scores.get(symbol).get('environmentScore')
        governance = ticker.esg_scores.get(symbol).get('governanceScore')
        social = ticker.esg_scores.get(symbol).get('socialScore')
    return environment, governance, social


def get_capm_expected_return(symbol, ticker):
    if not isinstance(ticker.summary_detail.get(symbol), dict):
        raise ValueError('no expected return - ' + symbol)
    beta = ticker.summary_detail.get(symbol).get('beta')
    if beta is None or np.isnan(beta):
        raise ValueError('no expected return - ' + symbol)
    return CANADA_RISK_FREE_RATE + (beta * (TSX_EXPECTED_RETURN - CANADA_RISK_FREE_RATE))


def get_price(symbol, ticker):
    if 'regularMarketPreviousClose' not in ticker.price.get(symbol).keys() or np.isnan(
            ticker.price.get(symbol).get('regularMarketPreviousClose')):
        raise ValueError('no price - ' + symbol)
    price = ticker.price.get(symbol).get('regularMarketPreviousClose')
    return price


def get_symbol(company):
    result = yq.search(company, country='canada', first_quote=True)
    if 'symbol' not in result.keys():
        raise ValueError('symbol not found - ' + company)
    return result['symbol']


def save_company_data(companies):
    infos = {}
    success_count = 0
    for company in companies:
        print("Gathering output for " + company)
        try:
            symbol = get_symbol(company)
            ticker = yq.Ticker(symbol)
            price = get_price(symbol, ticker)
            expected_return = get_capm_expected_return(symbol, ticker)
            cvar, var = get_risk(symbol)
            environment, governance, social = get_esg(symbol, ticker)
            infos[symbol] = {
                'ticker': symbol,
                'price': price,
                'return': expected_return,
                'cvar': cvar,
                'var': var,
                'environment': None if environment is None or np.isnan(environment) else environment,
                'governance': None if governance is None or np.isnan(governance) else governance,
                'social': None if social is None or np.isnan(social) else social,
            }
            success_count += 1
            print("currently " + str(success_count) + " valid data points")
        except ValueError as e:
            print(e)
            continue
    save_to_file(infos, success_count)


def main():
    companies = get_companies()
    save_company_data(companies)


if __name__ == "__main__":
    main()
