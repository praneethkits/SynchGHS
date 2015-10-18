#!/usr/bin/env python
"""This script runs the synchGHS algorithm."""
import sys
import time
import argparse
import logging
import reader
from edge import Edge


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

def attach_edges(edges, process_id, edge):
    """Attaches a edge to process."""
    if process_id in edges:
        edges[process_id].append(edge)
    else:
        edges[process_id] = [edge]
        
def create_edges(input_parser):
    """Created the required edges between all process."""
    process_ids = input_parser.ids
    edge_weights = input_parser.weights
    current_process_index = 0
    edges = {}
    for i in xrange(len(process_ids)):
        j = i+1
        while j<len(process_ids):
            edge_id = str(process_ids[i]) + "," + str(process_ids[j])
            edge = Edge(edge_id, str(process_ids[i]), str(process_ids[j]), edge_weights[i][j])
            attach_edges(edges, process_ids[i], edge)
            attach_edges(edges, process_ids[j], edge)
            j += 1
    return edges

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