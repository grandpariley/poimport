import csv
import json
import time

import yfinance as yf


def get_companies():
    companies = []
    with open('TSX Data - 2022-02-06.csv', 'r') as tsx_file:
        tsx = csv.reader(tsx_file, delimiter=";")
        for company in tsx:
            companies.append(company[0])
    return companies


def save_to_file(d, file_index):
    with open('data' + str(file_index) + '.json', 'w') as output:
        json.dump(d, output, indent=4)


def save_company_data(companies):
    infos = []
    file_index, count_enough_info, count_not_enough_info = 0, 0, 0
    for company in companies:
        print("Gathering data for " + company)
        time.sleep(1)  # so I don't overload yahoo finance
        ticker = yf.Ticker(company)
        info = ticker.info
        if "threeYearAverageReturn" not in info or not info["threeYearAverageReturn"]:
            count_not_enough_info += 1
            print("\tnot enough information! count at " + str(count_not_enough_info))
            continue
        infos.append(info)
        count_enough_info += 1
        print("\tenough information! count at " + str(count_enough_info))
        if len(infos) > 5:
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
