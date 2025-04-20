import asyncio
import csv
import json
from datetime import datetime

import numpy as np
import pandas as pd
import quantstats as qs
import requests
import yahooquery as yq

from cache import file_cache
import db
from scale import scale

# https://www.bankofcanada.ca/rates/interest-rates/corra/
# updated April 16th, 2025
CANADA_RISK_FREE_RATE = 2.7600
# 1Y rate of return for S&P/TSX Composite Index
# https://ycharts.com/indices/%5ETSX
# updated April 16th, 2025
TSX_EXPECTED_RETURN = 10.94


async def get_companies():
    companies = get_companies_from_tsx()
    already_found = list(dict(await db.fetch_data()).keys())
    companies = filter(lambda c: c not in already_found, companies)
    no_data = list(map(lambda d: d['symbol'], await db.fetch_no_data()))
    companies = filter(lambda c: c not in no_data, companies)
    return list(sorted(companies))


@file_cache('companies.json')
def get_companies_from_tsx():
    companies = set()
    with open('data/companies.csv', newline='') as csv_file:
        file = csv.reader(csv_file)
        for row in file:
            companies.add(row[2])
    return list(companies)


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
    return CANADA_RISK_FREE_RATE + (beta * (TSX_EXPECTED_RETURN - CANADA_RISK_FREE_RATE)), beta


def get_price(symbol, ticker):
    if (not isinstance(ticker.price.get(symbol), dict)
            or 'regularMarketPreviousClose' not in ticker.price.get(symbol).keys()
            or np.isnan(ticker.price.get(symbol).get('regularMarketPreviousClose'))):
        raise ValueError('no price - ' + symbol)
    price = ticker.price.get(symbol).get('regularMarketPreviousClose')
    if price <= 0.05:
        raise ValueError('penny stock - ' + symbol)
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


async def get_company_data(companies):
    retry = []
    success_count = 0
    esg_count = 0
    for company in companies:
        print("Gathering output for " + company)
        symbol = company
        try:
            symbol = get_symbol(company)
            ticker = yq.Ticker(symbol)
            price = get_price(symbol, ticker)
            expected_return, beta = get_capm_expected_return(symbol, ticker)
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
                'timestamp': datetime.now(),
                'beta': beta
            }
            success_count += 1
            if d['environment'] or d['social'] or d['governance']:
                esg_count += 1
            await db.insert_data(symbol, d)
            print("currently " + str(success_count) + " valid data points with " + str(esg_count) + ' esg data points')
        except ValueError as e:
            print('caught value error: ' + str(e))
            if str(e) == 'Expecting value: line 1 column 1 (char 0)':
                retry.append(company)
                print('adding "' + company + '" to retries. currently ' + str(len(retry)) + ' retries in the queue')
                continue
            await db.insert_no_data(company)
            await db.insert_no_data(symbol)
            continue
        except Exception as e:
            print(e)
            retry.append(company)
            print('adding "' + company + '" to retries. currently ' + str(len(retry)) + ' retries in the queue')
            continue
    return retry


async def save_company_data(companies):
    attempts = 0
    while len(companies) > 0:
        print('attempt: ' + str(attempts) + ' | number of companies: ' + str(len(companies)))
        failed_companies = await get_company_data(companies)
        companies = failed_companies
        attempts += 1


async def main():
    await db.clear_data()
    companies = await get_companies()
    await save_company_data(companies)
    await scale()


if __name__ == "__main__":
    asyncio.run(main())
