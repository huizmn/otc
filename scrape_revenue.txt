
# scrape the otcmarkts website and save the results to csv table.
# assuming symbols are stored in a csv file

import requests
from pprint import pprint
import json
import pandas
import logging
import sys
import csv
import time
import string
from decimal import Decimal

tbl = str.maketrans({"(":"-",")":"",",":""})

def money(x):
    # parse the dollar values, remove , ()
    if x== "-":
        x=""
        #replace - with empty
    try:
        return x.translate(tbl)
    except:
        my_logger.warning(x)
        my_logger.warning("!!cannot extract dollar values")
        return None

def fetch_page(symbol):
    ''' fetch one symbol at a time
    Returns:
      html: a list of values
    '''
    url = "https://backend.otcmarkets.com/otcapi/company/financials/income-statement/"+symbol+"?symbol="+symbol+"&duration=annual"
      
    s = json.loads(requests.get(url).text)  
    rows = []
 
    if not s:
        # sometime we get empty return, because no data
        # but strangely, it may use a different API in some cases.
        url = "https://backend.otcmarkets.com/internal-otcapi/financials/income-statement/"+symbol+"?symbol="+symbol+"&duration=annual"
      
        s = json.loads(requests.get(url).text)  

        if not s:
            my_logger.warning(symbol + " --- ")
            return rows
        else:
            my_logger.warning(symbol + " found alternate api ")


    if not isinstance(s,list):
        # or the symbol does not exist error
        # logging.debug("error getting json")
        my_logger.warning(symbol + ":" + s['message'])
    else:
        try:
            # normal output, but in case something is wrong
            for r in s:
                row = [
                    symbol,
                    money(r.get("totalRevenue")),
                    money(r.get("costOfRevenue")),
                    money(r.get("grossProfit")),
                    money(r.get("researchAndDevelopment")),
                    money(r.get("sellingGeneralAndAd")),
                    money(r.get("nonRecurring")),
                    money(r.get("otherOperatingExpenses")),
                    money(r.get("operatingIncome")),
                    money(r.get("totalOtherIncomeAndExpensesNet")),
                    money(r.get("earningsBeforeInterestAndTaxes")),
                    money(r.get("interestExpense")),
                    money(r.get("incomeBeforeTax")),
                    money(r.get("incomeTaxExpense")),
                    money(r.get("minorityInterestExpense")),
                    money(r.get("equityEarnings")),
                    money(r.get("netIncomeFromContinuingOperations")),
                    money(r.get("discontinuedOperations")),
                    money(r.get("extraordinaryItems")),
                    money(r.get("effectOfAccountingChanges")),
                    money(r.get("otherItems")),
                    money(r.get("netIncome")),
                    money(r.get("preferredStockAndOtherAdjustments")),
                    money(r.get("netIncomeApplicableToCommonShares")),
                    r.get("periodEndDate")]
                my_logger.warning(row)
                rows.append(row)
        except Exception as e:
            print(e)
            my_logger.warning(s)
            sys.exit(0)

    return rows


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print('Usage: python scrape.py csv_filename_for_symbols')
        sys.exit(0)
    
    my_logger = logging.getLogger('MyLogger')


    csv_filename = sys.argv[1]+".csv";

    # # test the fetch_page function
    # j = fetch_page("MNAT")
    # pprint(j)

    column_names = [
        'symbol',
        "totalRevenue",
        "costOfRevenue",
        "grossProfit",
        "researchAndDevelopment",
        "sellingGeneralAndAd",
        "nonRecurring",
        "otherOperatingExpenses",
        "operatingIncome",
        "totalOtherIncomeAndExpensesNet",
        "earningsBeforeInterestAndTaxes",
        "interestExpense",
        "incomeBeforeTax",
        "incomeTaxExpense",
        "minorityInterestExpense",
        "equityEarnings",
        "netIncomeFromContinuingOperations",
        "discontinuedOperations",
        "extraordinaryItems",
        "effectOfAccountingChanges",
        "otherItems",
        "netIncome",
        "preferredStockAndOtherAdjustments",
        "netIncomeApplicableToCommonShares",
        'periodEndDate']

    csv_writer = csv.writer(open(sys.argv[1]+"_result2.csv","w",newline=''))

    csv_writer.writerow(column_names)

    for line in open(csv_filename):
        
        rows = fetch_page(line.strip())
        # write to file
        for row in rows:
            csv_writer.writerow(row)
        # sleep to be polite
        time.sleep(0.25)
