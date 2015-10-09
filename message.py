#!/usr/bin/env python
""" The file defines the Message class. """
import logging


class Message(object):
    """Class Defines various types of messages."""
    def __init__(self):
        """Defines the required messages."""
        self.type=None
        self.msg = ""
        self.process = None
        self.edge_id = None
        
    def test_message(self, component_id):
        self.type = "test"
        self.msg = component_id
        return self

    def send_MWOE(self, edge_weight, edge_id, process_id):
        self.type = "ret_MWOE"
        self.msg = edge_weight
        self.process = process_id
        self.edge_id = edge_id

    def get_MWOE(self):
        self.type = "get_MWOE"
        return self

    def get_leader(self):
        self.type = "get_Leader"
        return self

    def send_Leader(self, leader):
        self.type = "ret_leader"
        self.process = leader
        return self
        
    