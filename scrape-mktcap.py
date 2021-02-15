
# scrape the otcmarkts website - symbol profile and save the results to csv table.
# assuming symbols are stored in a csv file
# https://backend.otcmarkets.com/otcapi/company/profile/full/RHHBY?symbol=RHHBY

import requests
from pprint import pprint
import os.path
import json
import pandas
import logging
import sys
import csv
import time
import string
from decimal import Decimal
from datetime import datetime

tbl = str.maketrans({"(":"-",")":"",",":""})

def money(x):
    # parse the dollar values, remove , ()
    if x== "-":
        x=""
    try:
        return x.translate(tbl)
    except:
        if x is not None:
            print(x)
            print("!!cannot extract dollar values")
        return None

def fetch_page(symbol):
    ''' fetch one symbol at a time
    Returns:
      html: a list of values
    '''
    url = "https://backend.otcmarkets.com/otcapi/company/profile/full/"+symbol+"?symbol="+symbol
      
    row = {}

    try:
        s = json.loads(requests.get(url).text)  
    except Exception as e:
        logger.error("Unexpected error:", sys.exc_info()[0])
        return {}

    if not isinstance(s,dict):
        # or the symbol does not exist error
        # logging.debug("error getting json")
        logger.error(symbol + ": unexpected response type")
        print(s)
        return {}
    elif s.get("status"):
        # or the symbol does not exist error
        logger.error(symbol + ": http status error")
        print(s)
        return {}
    else:
        print(symbol + ": found ")
        try:
            # normal output, but in case something is wrong

            row = [
                symbol,
                s.get("name"),
                s.get("estimatedMarketCap"),
                # datetime.utcfromtimestamp(s.get("estimatedMarketCapAsOfDate")).strftime('%Y-%m-%d')
                datetime.fromtimestamp(int(s.get("estimatedMarketCapAsOfDate"))/1000).strftime('%Y-%m-%d')
                ]
            print(row)
        except Exception as e:
            logger.error('Failed to extract values from json: '+ str(e))
            print(s)
            sys.exit(0)

    return row


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print('Usage: python scrape-mktcap.py inputfile')
        print('The inputfile is a plain text file with each row being a symbol')
        exit(0)
    
    # verify input symbole file

    if not os.path.exists(sys.argv[1]):
        print("The supplied input file (a csv file of symbols) does not exist!".format(sys.argv[1]))
        exit(1)

    csv_filename = sys.argv[1]
    outfile = ""


    try:
        outfile = os.path.splitext(csv_filename)[0] + "-mktcap-results.csv"
    except:
        print("Unexpected file name: {}!".format(csv_filename))
        exit(1)

    if os.path.exists(outfile):
        answer = ""
        while answer not in ['y','n']:
            answer = input("File {} already exists, overwrite? (y/n)".format(outfile)) 
            answer = str(answer).lower()

        if answer=="n":
            exit(0)
    
    logger = logging.getLogger('MyLogger')

    column_names = [
        'symbol',
        "name",
        "estimatedMarketCap",
        "estimatedMarketCapAsOfDate"]

    csv_writer = csv.writer(open(outfile,"w",newline=''))

    csv_writer.writerow(column_names)

    for line in open(csv_filename):
        
        row = fetch_page(line.strip())
        # write to file
        if row:
            try:
                csv_writer.writerow(row)
            except Exception as e:
                logger.error('Failed to write row: '+ str(e))
                print(row)
                sys.exit(1)
        # sleep to be polite
        print("\n----------------\n")
        time.sleep(0.25)
