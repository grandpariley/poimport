import csv
import json
import os
import time

import numpy as np
import quantstats as qs


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
    file_index, count_enough_info, count_not_enough_info = 0, 0, 0
    for company in companies:
        print("Gathering output for " + company)
        time.sleep(1)  # so I don't overload yahoo finance
        stock = qs.utils.download_returns(company)
        rt = qs.stats.avg_return(stock)
        cvar = qs.stats.cvar(stock)
        var = qs.stats.var(stock)
        if np.isnan(rt) or np.isnan(cvar) or np.isnan(var):
            continue
        infos.append({
            'ticker': company,
            'return': rt,
            'cvar': cvar,
            'var': var
        })
        if len(infos) > 100:
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
