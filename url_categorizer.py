#! /usr/bin/env python
import csv
import os
import argparse
from threading import Thread 
from categorizer_tasks import query_yahoo, query_alexa
def parse_args():
    parser = argparse.ArgumentParser('little script to gather the category labels given a list of sites')
    parser.add_argument('-a', '--alexa', action='store_true',
        help='Gathers category labels from alexa.com')
    parser.add_argument('-y', '--yahoo', action='store_true', 
        help='Gathers category labels from yahoo content analysis')
    parser.add_argument('-c', '--cache', help='Loads previously cached results and resume from there')
    parser.add_argument('-d', '--qdata', help='Specify quantcast data file', default='ads-hosts.quant')
    return parser.parse_args()

def parse_quantcast_file(fpath):
    with open(fpath, 'rb') as frdr:
        for line in frdr: 
            tokens = line.split()
            if len(tokens) > 1:
                yield line.split()[0]

def main(args):
    tr_list = []
    if not args.qdata or not os.path.exists(args.qdata):
        raise IOError("Quantcast data file doesn't exist or not provided")
    url_list = list(parse_quantcast_file(args.qdata))
    if args.yahoo:
        tr = Thread(target=query_yahoo, args=(url_list,))
        tr_list.append(tr)
        tr.start()
    if args.alexa:
        tr = Thread(target=query_alexa, args=(url_list,))
        tr_list.append(tr)
        tr.start()

    try :
        for tr in tr_list:
            tr.join()
        print "Process finished!"
    except KeyboardInterrupt:
        for i, tr in enumerate(tr_list):
            print "Killing process %d" % i 
            



if __name__ == "__main__":
    main(parse_args())
