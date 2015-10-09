#!/usr/bin/env python
"""This script runs the synchGHS algorithm."""
import sys
import time
import argparse
import logging
import reader


def main():
    """Program Starts here."""
    parser = argparse.ArgumentParser(prog='sync_ghs.py')
    parser.add_argument("-f", nargs=1, required=True,
                        help="File with the input data.")
    args = parser.parse_args()

    log_file = "logs/sync_ghs_%s.log" % time.strftime("%Y%m%d_%H%M%S")
    setup_log(log_file, logging.INFO)

    input_parser = reader.Reader(args.f[0])
    if not input_parser.read_data():
        logging.error("Parsing of input file failed.")
        sys.exit(-1)
    
def setup_log(log_file, level):
    """Sets up logging to file and console."""
    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    rootLogger = logging.getLogger()
    rootLogger.setLevel(level)
    
    fileHandler = logging.FileHandler(log_file)
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)
    fileHandler.setLevel(level)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.ERROR)
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)
    print 'Log is also stored at %s' % log_file

if __name__ == "__main__":
    main()