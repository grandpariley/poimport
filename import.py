import csv
import json
import os

import numpy as np
import quantstats as qs
import yfinance as yf


def get_companies():
    companies = []
    with open('TSX Data - 2022-02-06.csv', 'r') as tsx_file:
        tsx = csv.reader(tsx_file)
        for company in tsx:
            companies.append(company[0])
    return companies


def save_to_file(d, file_index):
    if not os.path.exists("output"):
        os.mkdir("output")
    with open('output/data' + str(file_index) + '.json', 'w') as output:
        json.dump(d, output, indent=4)


def save_company_data(companies):
    infos = []
    file_index = 0
    for company in companies:
        print("Gathering output for " + company)
        stock = qs.utils.download_returns(company)
        rt = qs.stats.avg_return(stock)
        cvar = qs.stats.cvar(stock)
        var = qs.stats.var(stock)
        if np.isnan(rt) or np.isnan(cvar) or np.isnan(var):
            continue
        ticker = yf.Ticker(company)
        environment, social, governance = float('nan'), float('nan'), float('nan')
        if ticker.sustainability is not None:
            environment = ticker.sustainability.get('Value').get('environmentScore')
            governance = ticker.sustainability.get('Value').get('governanceScore')
            social = ticker.sustainability.get('Value').get('socialScore')
        infos.append({
            'ticker': company,
            'return': rt,
            'cvar': cvar,
            'var': var,
            'environment': None if np.isnan(environment) else environment,
            'governance': None if np.isnan(governance) else governance,
            'social': None if np.isnan(social) else social,
        })
        if len(infos) > 200:
            print("Dumping to file")
            save_to_file(infos, file_index)
            file_index += 1
            infos = []
    save_to_file(infos, file_index)


def main():
    companies = get_companies()
    save_company_data(companies)


if __name__ == "__main__":
    main()
