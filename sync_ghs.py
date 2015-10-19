#!/usr/bin/env python
"""This script runs the synchGHS algorithm."""
import sys
import time
import argparse
import logging
import reader
import Queue
from edge import Edge
from process import Process
from threading import Thread, Lock


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
        
    edges = create_edges(input_parser)
    for process in edges:
        logging.info("\nProcess edges: %s\n", process)
        for edge in edges[process]:
            logging.info("edge created with following details")
            logging.info("edge id %s", edge.id)
            logging.info("source_process %s", edge.source_process)
            logging.info("destination process %s", edge.dest_process)
            logging.info("edge weight %s", str(edge.get_weight()))
    processes = create_process(input_parser.ids, edges)
    
    # Start the process.
    process_messages = {}
    process_threads = {}
    for process_id in input_parser.ids:
        p_q_f = Queue.Queue()
        p_q_b = Queue.Queue()
        p_t = Thread(name=process_id, target=processes[process_id].run, args=(p_q_f,p_q_b,))
        p_t.start()
        process_messages[process_id] = (p_q_f, p_q_b)
        process_threads[process_id] = p_t

    sent_process = set()
    completed = set()
    while len(completed) != len(process_messages):
        sent_process = set()
        for process_id in process_messages:
            p_q_f, _ = process_messages[process_id]
            p_q_f.put("start_round")
        time.sleep(2)
        print "\n\n\n\n\n"
        while len(sent_process) != len(process_messages):
            for process_id in [p for p in process_messages if p not in sent_process]:
                _, p_q_b = process_messages[process_id]
                try:
                    msg = p_q_b.get()
                    print msg
                    if msg == "r_completed":
                        sent_process.add(process_id)
                        logging.info("Process %s round completed.", process_id)
                    elif msg == "completed":
                        sent_process.add(process_id)
                        completed.add(process_id)
                        logging.info("Process %s completed.", process_id)
                except Exception as cause:
                    print cause
                    print "exception raised"
                    continue
            time.sleep(1)

    for process_id in process_messages:
        p_q = process_messages[process_id]
        p_q.add("stop")

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
            if edge_weights[i][j] == -1:
                j += 1
                continue
            edge_id = str(process_ids[i]) + "," + str(process_ids[j])
            edge = Edge(edge_id, str(process_ids[i]), str(process_ids[j]), edge_weights[i][j])
            attach_edges(edges, process_ids[i], edge)
            attach_edges(edges, process_ids[j], edge)
            j += 1
    return edges
    
def create_process(process_ids, edges):
    """creates the process"""
    processes = {}
    for process_id in process_ids:
        process = Process(process_id, edges[process_id])
        processes[process_id] = process
        logging.info("process %s is created.", process_id)
    return processes

def setup_log(log_file, level):
    """Sets up logging to file and console."""
    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] "
                                     "[%(levelname)-5.5s] [%(lineno)5d]"
                                     " %(message)s")
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