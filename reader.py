#!/usr/bin/env python
"""This script reads the given input from the file."""
import os
import logging


class Reader(object):
    """This class reads the input file. The input file should have following
       Details.
       1) n (The number of process of distributed system.)
       2) one array id[n] of size n; the ith element of this array gives the
          unique id of the process.
       3) one n*n symmetric matrix, weight[n,n] that gives the connectivity
          and edge weight information."""
    def __init__(self, input_file):
        """Initializes the required variables."""
        self.input_file = input_file
        self.no_process = 0
        self.ids = []
        self.weights = []

    def validate_input_file(self):
        """Validates if the given input file exists."""
        return os.path.isfile(self.input_file)

    def __read_lines__(self):
        """Returns the lines of the input file in a list."""
        fd = open(self.input_file, "r")
        lines = fd.readlines()
        fd.close()
        return lines

    def __set_number_of_process__(self, line):
        """Sets the number of process from given line."""
        try:
            self.no_process = int(line.strip())
        except ValueError:
            logging.error("Error while reading number of process: INVALID NUMBER.")
            return False
        except Exception as cause:
            logging.error("Error while reading number of process: %s", cause)
            return False
        logging.info("No of Process: %d", self.no_process)
        return True

    def __set_ids__(self, line):
        """Sets the process ids."""
        try:
            self.ids = [x for x in ' '.join(line.split()).split(" ")]
        except ValueError:
            logging.error("Error while reading process id's: INVALID NUMBER.")
            return False
        except Exception as cause:
            logging.error("Error while reading process id's: %s", cause)
            return False
        if len(self.ids) != self.no_process:
            logging.error("Given number of ids and process count doesn't match")
            return False
        logging.info(["Process ids: "]  + self.ids)
        return True

    def __set_edge_weights__(self, lines):
        """Parses the given lines and stores in matrix form."""
        status = True
        if len(lines) != self.no_process:
            status = False
            logging.error("Given Matrix doesn't have %d rows", self.no_process)
        index = 1
        for line in lines:
            row_weights = [float(x) for x in ' '.join(line.split()).split(" ")]
            if len(row_weights) != self.no_process:
                logging.error("Process %d doesn't have enough edge weights", index)
                status = False
            index += 1
            self.weights.append(row_weights)
        logging.info(["Edge Weights: "] + self.weights)
        return status

    def read_data(self):
        """Reads the data from the file and stores in corresponding variables."""
        read_status = True
        lines = self.__read_lines__()
        # Read the number of process
        if not self.__set_number_of_process__(lines[0]):
            read_status = False
            return False

        # Read the process ids.
        if not self.__set_ids__(lines[1]):
            read_status = False

        # Read edge weights.
        if not self.__set_edge_weights__(lines[2:]):
            read_status = False

        return read_status
