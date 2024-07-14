import json
import os
import time

import numpy as np
import pandas as pd
import quantstats as qs
import requests
import yahooquery as yq

from cache import file_cache

# https://www.bankofcanada.ca/rates/interest-rates/corra/
# updated July 13, 2024
CANADA_RISK_FREE_RATE = 4.80
# 1Y rate of return for S&P/TSX Composite Index
# https://ycharts.com/indices/%5ETSX
# updated July 13, 2024
TSX_EXPECTED_RETURN = 16.69


@file_cache('companies.json')
def get_companies():
    companies = set()
    tsx_raw = requests.get("https://www.tsx.com/json/company-directory/search/tsx/^*").json()
    for tsx in tsx_raw['results']:
        companies.add(tsx['symbol'])
        for instrument in tsx['instruments']:
            companies.add(instrument['symbol'])
    return list(sorted(companies))


def save_to_file(d):
    if not d:
        return
    if not os.path.exists("output"):
        os.mkdir("output")
    with open('output/data.json', 'w') as output:
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


def get_company_data(companies):
    data = {}
    success_count = 0
    failed_companies = []
    for company in companies:
        print("Gathering output for " + company)
        try:
            symbol = get_symbol(company)
            ticker = yq.Ticker(symbol)
            price = get_price(symbol, ticker)
            expected_return = get_capm_expected_return(symbol, ticker)
            cvar, var = get_risk(symbol)
            environment, governance, social = get_esg(symbol, ticker)
            data[symbol] = {
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
            if str(e) == 'Expecting value: line 1 column 1 (char 0)':
                failed_companies.append(company)
                print('adding "' + company + '" to failed companies. currently ' + str(len(failed_companies)) + ' failed fetches')
                time.sleep(30)
            continue
    return data, failed_companies


def save_company_data(companies):
    data, failed_companies = get_company_data(companies)
    attempts = 1
    while len(failed_companies) > 0 and attempts < 20:
        print('attempt: ' + str(attempts) + ' | number of failed fetches: ' + str(len(failed_companies)))
        new_data, new_failed_companies = get_company_data(failed_companies)
        data = data | new_data
        failed_companies = new_failed_companies
    return data


def main():
    companies = get_companies()
    save_to_file(save_company_data(companies))


if __name__ == "__main__":
    main()
