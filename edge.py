#!/usr/bin/env python
""" The file defines the edge class. """
import logging
from threading import Lock

class Edge(object):
    """Defines the properties of edge."""
    def __init__(self, id, source_process, dest_process, weight):
        """Variables required in edge class."""
        self.source_process = source_process
        self.dest_process = dest_process
        self.__weight__ = weight
        self.id = id
        self.forward_msg_lock = Lock()
        self.reverse_msg_lock = Lock()
        self.forward_messages = []
        self.reverse_messages = []
        
    def get_weight(self):
        """Returns the weight of edge."""
        return self.__weight__

    def send_message(self, message, process_id):
        """"send the messages from source to destination."""
        if process_id == self.source_process:
            self.forward_msg_lock.acquire()
            self.forward_messages.insert(0, message)
            self.forward_msg_lock.release()
        elif process_id == self.dest_process:
            self.reverse_msg_lock.acquire()
            self.reverse_messages.insert(0, message)
            self.reverse_msg_lock.release()
        else:
            logging.error("Unable to send message from given process_id: %s",
                          str(process_id))
            return False
        return True

    def receive_message(self, process_id):
        """Returns the tuple boolean and a message if available."""
        if process_id == self.dest_process:
            self.forward_msg_lock.acquire()
            try:
                msg = self.forward_messages.pop()
            except Exception as cause:
                msg = None
            self.forward_msg_lock.release()
            return (True, msg)
        elif process_id == self.dest_process:
            self.reverse_msg_lock.acquire()
            try:
                msg = self.reverse_messages.pop()
            except Exception as cause:
                msg = None
            self.reverse_msg_lock.release()
            return (True, msg)
        else:
            return (False, "")
 
