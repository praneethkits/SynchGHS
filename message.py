#!/usr/bin/env python
""" The file defines the Message class. """
import logging


class Message(object):
    """Class Defines various types of messages."""
    def __init__(self):
        """Defines the required messages."""
        self.type = None
        self.msg = ""
        self.process = None
        self.edge_id = None
        
    def test_message(self, component_id):
        self.type = "test"
        self.msg = component_id
        return self

    def ret_test_message(self, component_id):
        self.type = "ret_test"
        self.msg = component_id
        return self

    def send_MWOE(self, edge_weight, edge_id, process_id):
        self.type = "ret_MWOE"
        self.msg = edge_weight
        self.process = process_id
        self.edge_id = edge_id
        return self

    def merge_MWOE(self, edge_id, process_id):
        self.type = "merge_MWOE"
        self.msg = "merge with mwoe"
        self.process = process_id
        self.edge_id = edge_id
        return self

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

    def merge_request(self, level_id, component_id):
        self.type = "merge"
        self.level = level_id
        self.component_id = component_id
        return self

    def update_component(self, level_id, component_id):
        self.type = "update_component"
        self.level = level_id
        self.component_id = component_id
        return self

    def acknowledge_leader(self):
        self.type = "ack_leader"
        self.msg = "updated cmponent"
        return self

    def merge_request_to_leader(self, msg):
        self.type = "merge_to_leader"
        self.msg = msg
        return self
    