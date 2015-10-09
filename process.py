#!/usr/bin/env python
"""Constructs the process class."""
import edge
from mesaage import Message
import time


class Process(object):
    """Defines the task of individual process in distributed system."""
    def __init__(self, process_id, edges):
        """Initializes the required variables for the process class."""
        self.process_id = process_id
        sel.component_id = process_id
        self.edges = edges
        self.parent_edge = None
        self.level = 0
        self.mst_edges = []
        self.non_mst_edges = []
        self.edges_index = {}
        self.leader = True
        self.index_edges()
        
    def index_edges(self):
        for edge in self.edges:
            if edge.source_process == self.process_id
                self.edges_index[edge.dest_process] = edge
            else:
                self.edges_index[edge.source_process] = edge

    def broadcast_messages(self, message):
        """Broadcasts the given message among all edges."""
        for edge in self.edges:
            edge.send_message(self.process_id, message)
            
    def get_min_edge(self):
        """This function returns the minimum edge of all the incident edges."""
        min_edge = None
        min_weight = 1000000
        for edge in self.non_mst_edges:
            if edge.get_weight() <= min_weight:
                min_weight = edge.get_weight()
                min_edge = edge
        return edge

    def get_MWOE(self):
        msg = Message()
        msg = msg.get_MWOE()
        for edge in self.mst_edges:
            if edge == self.parent_edge
                continue
            edge.send_message(self.process_id, msg)
        recieved_mwoe_msgs = []
        while len(recieved_mwoe_edges) != len(self.mst_edges) - 1:
            for edge in self.mst_edges:
                status, msg = edge.receive_message(self.process_id)
                if not status:
                    continue
                recieved_mwoe_msgs.append(msg)
            time.sleep(0.5)
        min_edge_id = None
        min_weight = 1000000
        min_process_id = None
        for msg in recieved_mwoe_msgs:
            if msg.msg < min_weight:
                min_weight = msg.msg
                min_edge_id = msg.edge_id
                min_process_id = msg.process_id
        for edge in self.non_mst_edges:
            if edge.get_weight() < min_weight:
                min_weight = edge.get_weight()
                min_edge_id = edge_id
                min_process_id = self.process_id
        ret_msg = Message()
        ret_msg = ret_msg.send_MWOE(min_weight, min_edge_id, self.process_id)
        return ret_msg

    def find_MWOE(self):
        """This function finds the MWOE of all the edges."""
        if self.level == 0 or (len(self.mst_edges) -1) == 0 and \
                self.parent_edge is None) or len(self.mst_edges) == 0:
            edge = self.get_min_edge()
            ret_msg = Message()
            ret_msg = ret_msg.send_MWOE(edge.get_weight(), edge.id, self.process_id)
            return ret_msg
        else:
            return get_MWOE()
            
    def get_non_mst_edges(self):
        """This function is used to find the non mst edges."""
        self.non_mst_edges = []
        msg = Message()
        msg = msg.test_message(self.component_id)
        self.broadcast_messages(msg)
        rcvd_msgs = 0
        while rcvd_msgs != len(self.edges):
            
            
            
    def send_join_request(self):
        """Sends the join request to the MWOE."""
        
    
    