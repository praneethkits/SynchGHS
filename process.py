#!/usr/bin/env python
"""Constructs the process class."""
import edge
from message import Message
import time
from threading import Thread, Lock


class Process(object):
    """Defines the task of individual process in distributed system."""
    def __init__(self, process_id, edges):
        """Initializes the required variables for the process class."""
        self.process_id = process_id
        self.component_id = process_id
        self.edges = edges
        self.parent_edge = None
        self.level = 0
        self.mst_edges = []
        self.non_mst_edges = []
        self.edges_index = {}
        self.leader = True
        self.index_edges()
        self.messages_lock = Lock()
        self.messages = {}
        
    def get_edge_processid(self, edge):
        """Returns the other end process id of edge."""
        if edge.source_process == self.process_id:
            return edge.dest_process
        else:
            return edge.source_process

    def setup_messages(self):
        """sets up the messages."""
        for edge in self.edges:
            self.messages[edge.id] = {}
        
    def message_listener(self):
        """Listens for the message from other process and stores the messages
        as required."""
        while True:
            for edge in self.edges:
                msg = edge.receive_message(self.process_id)
                if msg is None:
                    continue
                if msg.type == "test":
                    ret_msg = Message()
                    edge.send_message(ret_msg.ret_test_message(self.component_id),
                                      self.process_id)
                    logging.info("Test message returned to process: %s",
                                 str(self.get_edge_processid(edge)))
                    continue
                self.messages_lock.acquire()
                if msg.type in self.messages[edge.id]:
                    self.messages[edge.id][msg.type].append(msg)
                else:
                    self.messages[edge.id][msg.type] = [msg]
                self.messages_lock.release()
            time.sleep(1)

    def index_edges(self):
        for edge in self.edges:
            if edge.source_process == self.process_id:
                self.edges_index[edge.dest_process] = edge
            else:
                self.edges_index[edge.source_process] = edge

    def broadcast_messages(self, message):
        """Broadcasts the given message among all edges."""
        for edge in self.edges:
            edge.send_message(message, self.process_id)
            
    def get_min_edge(self):
        """This function returns the minimum edge of all the incident edges."""
        min_edge = None
        min_weight = 1000000
        for edge in self.non_mst_edges:
            if edge.get_weight() <= min_weight:
                min_weight = edge.get_weight()
                min_edge = edge
        return min_edge

    def get_MWOE(self):
        msg = Message()
        msg = msg.get_MWOE()
        for edge in self.mst_edges:
            if edge == self.parent_edge:
                continue
            edge.send_message(msg, self.process_id)
        recieved_mwoe_msgs = []
        seen_edges = []
        if self.parent_edge:
            seen_edges.append(self.parent_edge)
        while len(seen_edges) != len(self.mst_edges):
            for edge in [e for e in self.mst_edges if e not in seen_edges]:
                self.messages_lock.acquire()
                if "ret_MWOE" in self.messages[edge.id] and\
                        len(self.messages[edge.id]["ret_MWOE"]) != 0:
                    mwoe_msg = self.messages[edge.id]["ret_MWOE"].pop()
                    recieved_mwoe_edges.append(mwoe_msg)
                    msgs_recvd += 1
                    seen_edges.append(edge)
                self.messages_lock.release()
            time.sleep(0.5)
        min_edge_id = None
        min_weight = 1000000
        min_process_id = None
        for msg in recieved_mwoe_msgs:
            if msg.msg < min_weight:
                min_weight = msg.msg
                min_edge_id = msg.edge_id
                min_process_id = msg.process_id
        current_min_edge = self.get_min_edge()
        if current_min_edge.get_weight() < min_weight:
            min_weight = current_min_edge.get_weight()
            min_edge_id = current_min_edge.id
            min_process_id = self.process_id
        ret_msg = Message()
        ret_msg = ret_msg.send_MWOE(min_weight, min_edge_id, min_process_id)
        return ret_msg

    def find_MWOE(self):
        """This function finds the MWOE of all the edges."""
        if self.level == 0 or (len(self.mst_edges) -1 == 0 and \
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
        seen_edges = []
        while len(seen_edges) != len(self.edges):
            for edge in [e for e in self.edges if e not in seen_edges]:
                self.messages_lock.acquire()
                if "ret_test" in self.messages[edge.id]:
                    msg = self.messages[edge.id]["ret_test"].pop()
                    if msg.msg != self.component_id:
                        self.non_mst_edges.append(edge)
                    seen_edges.append(edge)
                self.messages_lock.release()
            time.sleep(0.5)

    def send_join_request(self, MWOE):
        """Sends the join request to the MWOE."""
        msg = Message()
        msg = msg.merge_request(self.level, self.component_id)
        if not MWOE.send_message(msg, self.process_id):
            logging.info("Merge Request from process_id: %s through" +
                         " edge %s failed", str(self.process_id), str(MWOE.id))
            return False
        return True
        
    def update_component_id(self, component_id):
        self.component_id = component_id
        
    def update_leader(self, leader):
        self.leader = leader
        
    def unite_forest(self, msg):
        """This fuunction is called by leader process to update the component
        of all process in the current forest."""
        if msg.component_id > self.component_id:
            logging.info("Process %s is setting as non leader.",
                         str(self.process_id))
            self.update_leader(False)
        else:
            if self.level == msg.level:
                self.level += 1
            msg = Message()
            msg = msg.update_component(self.level, self.component_id)
            for edge in self.mst_edges:
                edge.send_message(msg, self.process_id)
            edges_seen = []
            while len(edges_seen) != len(self.mst_edges):
                for edge in [e for e in self.mst_edges if e not in seen_edges]:
                    self.messages_lock.acquire()
                    if "ack_leader" in self.messages[edge.id]:
                        msg = self.messages[edge.id]["ack_leader"].pop()
                        seen_edges.append(edge)
                    self.messages_lock.release()
    
    def merge(self, msg):
        """Merges two forests in the spanning tree."""
        if self.leader:
            self.unite_forest(msg)
        else:
            new_msg = Message()
            new_msg = new_msg.merge_request_to_leader(msg)
            self.parent_edge.send_message(new_msg, self.process_id)

    def work_on_merge_to_leader_msg(self, msg):
        """Process the merge message from the child process."""
        if self.leader:
            logging.info("Leader process %s received the merge request.",
                         str(self.process_id))
            actual_msg = msg.msg
            self.unite_forest(actual_msg)
        else:
            logging.info("Message to merge from child is sent to parent " +
                         "by process %s", str(self.process_id))
            self.parent_edge.send_message(msg, self.process_id)

    def process_update_component_msg(self, msg, in_edge):
        """Process the update component msg from the leader."""
        self.level = msg.level
        self.component_id = msg.component_id
        self.parent_edge = in_edge
        for edge in [e for e in self.mst_edges if e != in_edge]:
            edge.send_message(msg, self.process_id)
        seen_edges = [in_edge]
        while len(seen_edges) != len(mst_edges):
            for edge in [e for e in self.mst_edges if e not in seen_edges]:
                self.messages_lock.acquire()
                if "ack_leader" in self.messages[edge.id]:
                    ack_msg = self.messages[edge.id]["ack_leader"].pop()
                    seen_edges.append(edge)
                self.messages_lock.release()
            time.sleep(0.5)
        ack_msg = Message()
        ack_msg = ack_msg.acknowledge_leader()
        self.parent_edge.send_message(ack_msg, self.process_id)

    
    