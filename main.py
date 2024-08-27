import json
import os
from shutil import copyfile

import numpy as np
import pandas as pd
import quantstats as qs
import requests
import yahooquery as yq

from cache import file_cache
from scale import scale
from validation import validate

# https://www.bankofcanada.ca/rates/interest-rates/corra/
# updated July 13, 2024
CANADA_RISK_FREE_RATE = 4.5300
# 1Y rate of return for S&P/TSX Composite Index
# https://ycharts.com/indices/%5ETSX
# updated August 26th, 2024
TSX_EXPECTED_RETURN = 20.98


def save(obj, filename):
    with open(filename, 'w') as json_file:
        json.dump(obj, json_file)


def fetch(filename):
    if not os.path.exists(filename):
        return None
    with open(filename, 'r') as json_file:
        return json.load(json_file)


@file_cache('companies.json')
def get_companies_from_tsx():
    companies = set()
    tsx_raw = requests.get("https://www.tsx.com/json/company-directory/search/tsx/^*").json()
    for tsx in tsx_raw['results']:
        companies.add(tsx['symbol'])
        for instrument in tsx['instruments']:
            companies.add(instrument['symbol'])
    return list(companies)


def get_companies():
    companies = get_companies_from_tsx()
    data = fetch('output/data.json')
    if data:
        companies = filter(lambda c: c not in dict(data).keys(), companies)
    no_data = fetch('no_data.json')
    if no_data:
        companies = filter(lambda c: c not in list(no_data), companies)
    return list(sorted(companies))


def save_data(data, fileprefix=''):
    if not data:
        return
    if not os.path.exists("output"):
        os.mkdir("output")
    existing = fetch('output/' + fileprefix + 'data.json')
    if existing:
        data = {**dict(existing), **data}
    save(data, 'output/' + fileprefix + 'data.json')


def get_risk(symbol):
    returns = qs.utils.download_returns(symbol)
    if returns is None or not isinstance(returns, pd.Series):
        raise ValueError('cannot find risk - ' + symbol)
    var = qs.stats.var(returns)
    cvar = qs.stats.cvar(returns)
    if var is None or np.isnan(var) or cvar is None or np.isnan(cvar):
        raise ValueError('cannot find risk - ' + symbol)
    return var, cvar


def get_esg(symbol, ticker):
    environment, governance, social = np.nan, np.nan, np.nan
    if not isinstance(ticker.esg_scores.get(symbol), str):
        environment = ticker.esg_scores.get(symbol).get('environmentScore')
        social = ticker.esg_scores.get(symbol).get('socialScore')
        governance = ticker.esg_scores.get(symbol).get('governanceScore')
    return environment, social, governance


def get_capm_expected_return(symbol, ticker):
    if not isinstance(ticker.summary_detail.get(symbol), dict):
        raise ValueError('no expected return - ' + symbol)
    beta = ticker.summary_detail.get(symbol).get('beta')
    if symbol == '^GSPTSE':
        beta = 0.5  # the S&P TSX index should have a beta of 0.5 by definition
    if beta is None or np.isnan(beta):
        raise ValueError('no expected return - ' + symbol)
    return CANADA_RISK_FREE_RATE + (beta * (TSX_EXPECTED_RETURN - CANADA_RISK_FREE_RATE))


def get_price(symbol, ticker):
    if (not isinstance(ticker.price.get(symbol), dict)
            or 'regularMarketPreviousClose' not in ticker.price.get(symbol).keys()
            or np.isnan(ticker.price.get(symbol).get('regularMarketPreviousClose'))):
        raise ValueError('no price - ' + symbol)
    price = ticker.price.get(symbol).get('regularMarketPreviousClose')
    return price


def get_symbol(company):
    result = yq.search(company, country='canada', first_quote=True)
    if 'symbol' not in result.keys():
        raise ValueError('symbol not found - ' + company)
    symbol = result['symbol']
    if symbol != company:
        with open('companies.json', 'r') as json_file:
            companies = list(json.load(json_file))
            companies[companies.index(company)] = symbol
        with open('companies.json', 'w') as json_file:
            json.dump(companies, json_file)
    return symbol


def get_company_data(companies, retry, fileprefix=''):
    success_count = 0
    esg_count = 0
    for company in companies:
        print("Gathering output for " + company)
        symbol = company
        try:
            symbol = get_symbol(company)
            ticker = yq.Ticker(symbol)
            price = get_price(symbol, ticker)
            expected_return = get_capm_expected_return(symbol, ticker)
            cvar, var = get_risk(symbol)
            environment, social, governance = get_esg(symbol, ticker)
            d = {
                'ticker': symbol,
                'price': price,
                'return': expected_return,
                'cvar': cvar,
                'var': var,
                'environment': None if environment is None or np.isnan(environment) else environment,
                'social': None if social is None or np.isnan(social) else social,
                'governance': None if governance is None or np.isnan(governance) else governance,
            }
            success_count += 1
            if d['environment'] or d['social'] or d['governance']:
                esg_count += 1
            save_data({symbol: d}, fileprefix)
            print("currently " + str(success_count) + " valid data points with " + str(esg_count) + ' esg data points')
        except ValueError as e:
            print(e)
            if str(e) == 'Expecting value: line 1 column 1 (char 0)':
                retry.append(company)
                print('adding "' + company + '" to retries. currently ' + str(len(retry)) + ' retries in the queue')
                continue
            no_data = [company, symbol]
            curr_no_data = fetch('no_data.json')
            if curr_no_data:
                no_data += curr_no_data
            save(list(sorted(set(no_data))), 'no_data.json')
            continue
        except Exception as e:
            print(e)
            retry.append(company)
            print('adding "' + company + '" to retries. currently ' + str(len(retry)) + ' retries in the queue')
            continue
    return retry


def save_company_data(companies, fileprefix=''):
    attempts = 0
    while len(companies) > 0:
        print('attempt: ' + str(attempts) + ' | number of companies: ' + str(len(companies)))
        failed_companies = get_company_data(companies, [], fileprefix)
        companies = failed_companies
        attempts += 1


def main():
    save_company_data(['GSPTSE'], 'index-')
    save_company_data(get_companies())
    os.mkdir('output/raw')
    copyfile('output/data.json', 'output/raw/data.json')
    scale()
    validate()
    scale(file='output/index-data.json')


if __name__ == "__main__":
    main()
