import csv
import json
import os

import numpy as np
import quantstats as qs
from yahooquery import Ticker


def get_companies():
    companies = []
    with open('TSX-tickers.csv', 'r') as tsx_file:
        tsx = csv.reader(tsx_file)
        for company in tsx:
            companies.append(company[0])
    return companies


def save_to_file(d, file_index):
    if not os.path.exists("output"):
        os.mkdir("output")
    with open('output/data' + str(file_index) + '.json', 'w') as output:
        json.dump(d, output)


def save_company_data(companies):
    infos = {}
    file_index = 0
    for company in companies:
        print("Gathering output for " + company)
        # try:
        if 1 == 1:
            stock = qs.utils.download_returns(company, period="1mo")
            if stock.empty:
                print("delisted stock")
                continue
            rt = qs.stats.expected_return(stock)
            cvar = qs.stats.cvar(stock)
            var = qs.stats.var(stock)
            if np.isnan(rt) or np.isnan(cvar) or np.isnan(var):
                print("not a number " + str(rt) + " | " + str(cvar) + " | " + str(var))
                continue
            ticker = Ticker(company)
            environment, social, governance = float('nan'), float('nan'), float('nan')
            if 'regularMarketPreviousClose' not in ticker.price.get(company).keys():
                print("no price")
                continue
            price = ticker.price.get(company)['regularMarketPreviousClose']
            if np.isnan(price):
                print("no price " + str(price))
                continue
            if not isinstance(ticker.esg_scores.get(company), str):
                environment = ticker.esg_scores.get(company).get('environmentScore')
                governance = ticker.esg_scores.get(company).get('governanceScore')
                social = ticker.esg_scores.get(company).get('socialScore')
            infos[company] = {
                'ticker': company,
                'price': price,
                'return': rt,
                'cvar': cvar,
                'var': var,
                'environment': None if environment is None or np.isnan(environment) else environment,
                'governance': None if governance is None or np.isnan(governance) else governance,
                'social': None if social is None or np.isnan(social) else social,
            }
        # except:
        #     continue
        # if len(infos) > 1:
        #     print("Dumping to file")
        #     save_to_file(infos, file_index)
        #     file_index += 1
        #     infos = []
    save_to_file(infos, file_index)


def main():
    companies = get_companies()
    save_company_data(companies)


if __name__ == "__main__":
    main()
