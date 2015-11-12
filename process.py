#!/usr/bin/env python
"""Constructs the process class."""
__author__ = "Praneeth Valem, Divyendra Kumar, Jonathan Tey"

import logging
import edge
from message import Message
import time
from threading import Thread, Lock
import inspect


class Process(object):
    """Defines the task of individual process in distributed system."""
    def __init__(self, process_id, edges):
        """Initializes the required variables for the process class."""
        self.process_id = process_id
        self.component_id = process_id
        self.edges = edges
        self.parent_edge = None
        self.level = 0
        self.mst_edges = set()
        self.non_mst_edges = []
        self.edges_index = {}
        self.edgeid_index = {}
        self.leader = True
        self.index_edges()
        self.messages_lock = Lock()
        self.messages = {}
        self.setup_messages()
        self.run_listener = True
        self.merge_mwoe_received_lock = Lock()
        self.merge_mwoe_received = None
        self.round_completed = False
        self.completed = False
        
    def log_mst_edges(self):
        logging.info("MST EDGES for process: %s", self.process_id)
        for edge in self.mst_edges:
            logging.info(str(edge))

    def run(self, queue_f, queue_b):
        """Process starts here."""
        logging.info("Process Id: %s", self.process_id)
        logging.info("Number of adjacent process = %d", len(self.edges))
        listner_t = Thread(name=self.process_id + ".listener",
                           target=self.message_listener)
        listner_t.start()
        while True:
            msg = queue_f.get()
            logging.info("Message from main thread = %s", msg)
            if msg == "start_round":
                self.run_round()
                while not self.round_completed:
                    time.sleep(1)
                if not self.completed:
                    logging.info("sending round completed message.")
                    queue_b.put("r_completed")
                else:
                    queue_b.put("completed")
            elif msg == "stop":
                break
            adjacency_list = []
            for edge in self.mst_edges:
                adjacency_list.append(self.get_edge_processid(edge))
            logging.info("Process %s, Adjacency list = %s, component_id ="
                         " %s, level=%d, leader=%s",
                         self.process_id, ", ".join(adjacency_list),
                         self.component_id, self.level, str(self.leader))
            time.sleep(1)
        self.run_listener = False
        listner_t.join()
        adjacency_list = []
        for edge in self.mst_edges:
            adjacency_list.append(self.get_edge_processid(edge))
        print "Process %s, Adjacency list = %s, component_id = %s"\
            %(self.process_id, ", ".join(adjacency_list), self.component_id)
        logging.info("Process completed.")
        return
        
    def run_round(self):
        """Runs the current round before rporting back."""
        self.round_completed = False
        self.merge_mwoe_received = None
        self.clear_ret_messages()
        self.get_non_mst_edges()
        if self.leader:
            logging.info("finding mwoe")
            mwoe_msg = self.find_MWOE()
            logging.info("MWOE details: edge id: %s, weight: %f",
                         mwoe_msg.edge_id, mwoe_msg.msg)
            if mwoe_msg.edge_id is None:
                self.completed = True
                self.round_completed = True
                completed_msg = Message()
                completed_msg = completed_msg.get_completed_msg()
                for edge in self.mst_edges:
                    edge.send_message(completed_msg, self.process_id)
                return

            self.merge_mwoe_received_lock.acquire()                
            self.merge_mwoe_received = mwoe_msg
            self.merge_mwoe_received_lock.release()
            # code for leader to send the mwoe request to its child
            merge_mwoe_msg = Message()
            merge_mwoe_msg = merge_mwoe_msg.merge_MWOE(mwoe_msg.edge_id,
                                                       mwoe_msg.process)
            self.log_mst_edges()
            for edge in self.mst_edges:
                edge.send_message(merge_mwoe_msg, self.process_id)

            if mwoe_msg.process == self.process_id:
                self.send_join_request(self.get_edge(mwoe_msg.edge_id))
                self.merge_mwoe(mwoe_msg)

    def get_edge(self, edge_id):
        """Returns the edge with the given edge_id"""
        return self.edgeid_index[edge_id]
        
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
            
    def clear_ret_messages(self):
        """Clears all the ret messages."""
        for edge_id in self.messages:
            if "ret_test" in self.messages[edge_id]:
                self.messages[edge_id].pop("ret_test")
        
    def message_listener(self):
        """Listens for the message from other process and stores the messages
        as required."""
        logging.info("listener Started.")
        sub_threads = []
        while True:
            logging.debug(self.messages)
            if not self.run_listener:
                break
            for edge in self.edges:
                status, msg = edge.receive_message(self.process_id)
                if msg is None:
                    continue
                if not status:
                    logging.warning("Unable to receive message")
                    continue
                logging.info("Message type: %s", msg.type)
                if msg.type == "test":
                    ret_msg = Message()
                    edge.send_message(ret_msg.ret_test_message(self.component_id),
                                      self.process_id)
                    logging.debug("Test message returned to process: %s",
                                 str(self.get_edge_processid(edge)))
                    continue
                elif msg.type == "update_component":
                    sub_t = Thread(name=self.process_id + ".l.upd_comp",
                                      target=self.process_update_component_msg,
                                      args=(msg, edge,))
                    sub_t.start()
                    sub_threads.append(sub_t)
                    continue
                elif msg.type == "get_MWOE":
                    sub_t = Thread(name=self.process_id + ".l.mwoe",
                                   target=self.handle_get_MWOE,
                                   args=(edge,))
                    sub_t.start()
                    sub_threads.append(sub_t)
                elif msg.type == "merge_MWOE":
                    self.merge_mwoe_received_lock.acquire()
                    self.merge_mwoe_received = msg
                    self.merge_mwoe_received_lock.release()
                    sub_t = Thread(name=self.process_id + ".l.merge",
                                   target=self.handle_merge_mwoe,
                                   args=(msg,))
                    sub_t.start()
                    sub_threads.append(sub_t)
                    continue
                elif msg.type == "merge":
                    sub_t = Thread(name=self.process_id + ".l.merge",
                                   target=self.handle_merge_request,
                                   args=(msg, edge,))
                    sub_t.start()
                    sub_threads.append(sub_t)
                    # this msg should also be in messages dict, hence no
                    # continue statement here.
                elif msg.type == "merge_to_leader":
                    sub_t = Thread(name=self.process_id + ".l.merge_leader",
                                   target=self.work_on_merge_to_leader_msg,
                                   args=(msg,))
                    sub_t.start()
                    sub_threads.append(sub_t)
                    continue
                elif msg.type == "deny_merge_upward":
                    self.round_completed = True
                    for ed in [ e for e in self.mst_edges if e != edge ]:
                        ed.send_message(msg, self.process_id)
                    continue
                elif msg.type == "completed":
                    self.completed = True
                    self.round_completed = True
                    for ed in [e for e in self.mst_edges if e != edge]:
                        ed.send_message(msg, self.process_id)
                    continue
                self.messages_lock.acquire()
                if msg.type in self.messages[edge.id]:
                    self.messages[edge.id][msg.type].append(msg)
                else:
                    self.messages[edge.id][msg.type] = [msg]
                self.messages_lock.release()
            time.sleep(1)
        for t in sub_threads:
            t.join()

    def index_edges(self):
        for edge in self.edges:
            self.edgeid_index[edge.id] = edge
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
            if edge.get_weight() < min_weight or \
                    (edge.get_weight() == min_weight and edge.id < min_edge.id):
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
                    recieved_mwoe_msgs.append(mwoe_msg)
                    seen_edges.append(edge)
                self.messages_lock.release()
            time.sleep(0.5)
        min_edge_id = None
        min_weight = 1000000
        min_process_id = None
        for msg in recieved_mwoe_msgs:
            if msg.msg < min_weight or (msg.msg == min_weight and msg.edge_id < min_edge_id):
                min_weight = msg.msg
                min_edge_id = msg.edge_id
                min_process_id = msg.process
        if self.process_id == "G":
            logging.info("Received mwoe details: min_weight=%f, min_edge_id=%s",min_weight,min_edge_id)
        current_min_edge = self.get_min_edge()
        if current_min_edge and (current_min_edge.get_weight() < min_weight or 
                (current_min_edge.get_weight() == min_weight and current_min_edge.id < min_edge_id)):
            min_weight = current_min_edge.get_weight()
            min_edge_id = current_min_edge.id
            min_process_id = self.process_id
        ret_msg = Message()
        ret_msg = ret_msg.send_MWOE(min_weight, min_edge_id, min_process_id)
        logging.info("MWOE Details: edge_id %s, edge_weight %f, process: %s",
                     min_edge_id, min_weight, min_process_id)
        return ret_msg
        
    def handle_get_MWOE(self, edge):
        """Handle the get MWOE request."""
        edge.send_message(self.find_MWOE(), self.process_id)

    def find_MWOE(self):
        """This function finds the MWOE of all the edges."""
        if self.level == 0 or (len(self.mst_edges) == 0 and \
                self.parent_edge is None):
            edge = self.get_min_edge()
            ret_msg = Message()
            ret_msg = ret_msg.send_MWOE(edge.get_weight(), edge.id, self.process_id)
            return ret_msg
        else:
            return self.get_MWOE()

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
                if "ret_test" in self.messages[edge.id] and\
                        len(self.messages[edge.id]["ret_test"]):
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
        logging.info("Sending Join request on edge: %s", MWOE.id)
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
            elif self.level < msg.level:
                self.level = msg.level
            msg = Message()
            msg = msg.update_component(self.level, self.component_id)
            for edge in self.mst_edges:
                edge.send_message(msg, self.process_id)
            seen_edges = []
            self.log_mst_edges()
            while len(seen_edges) != len(self.mst_edges):
                for edge in [e for e in self.mst_edges if e not in seen_edges]:
                    self.messages_lock.acquire()
                    if "ack_leader" in self.messages[edge.id] and\
                            len(self.messages[edge.id]["ack_leader"]):
                        logging.info("acknowledge received from edge: %s", edge.id)
                        msg = self.messages[edge.id]["ack_leader"].pop()
                        seen_edges.append(edge)
                    self.messages_lock.release()
            self.round_completed = True
    
    def merge(self, msg):
        """Merges two forests in the spanning tree."""
        if self.leader:
            self.unite_forest(msg)
        else:
            new_msg = Message()
            new_msg = new_msg.merge_request_to_leader(msg)
            self.parent_edge.send_message(new_msg, self.process_id)

    def merge_mwoe(self, mwoe_msg):
        while True:
            self.messages_lock.acquire()
            if "merge" in self.messages[mwoe_msg.edge_id] and\
                    len(self.messages[mwoe_msg.edge_id]['merge']):
                logging.info("Merge message received from edge %s", mwoe_msg.edge_id)
                self.mst_edges.add(self.get_edge(mwoe_msg.edge_id))
                merge_msg = self.messages[mwoe_msg.edge_id]['merge'].pop()
                self.messages_lock.release()
                self.merge(merge_msg)
                break
            elif "deny_merge" in self.messages[mwoe_msg.edge_id] and\
                    len(self.messages[mwoe_msg.edge_id]['deny_merge']):
                logging.info("Deny merge message recieved for this round from edge %s.", mwoe_msg.edge_id)
                deny_msg = self.messages[mwoe_msg.edge_id]['deny_merge'].pop()
                deny_msg = deny_msg.deny_merge_upward()
                self.round_completed = True
                self.messages_lock.release()
                for edge in self.mst_edges:
                    edge.send_message(deny_msg, self.process_id)
                break        
            self.messages_lock.release()
            time.sleep(1)

    def handle_merge_request(self, msg, edge):
        """Handles the merge mwoe message."""
        logging.info("Merge message recieved from %s", edge.id)
        self.merge_mwoe_received_lock.acquire()
        k = False
        while self.merge_mwoe_received is None:
            self.merge_mwoe_received_lock.release()
            k = True
            logging.info("process %s waiting to handle merge request from edge: %s", self.process_id, edge.id)
            time.sleep(1)
            self.merge_mwoe_received_lock.acquire()
        self.merge_mwoe_received_lock.release()
        if k:
            logging.info("process %s waiting to handle merge request finished", self.process_id)
        logging.info("merge mwoe received from edgeid = %s", self.merge_mwoe_received.edge_id)
        if self.merge_mwoe_received.edge_id != edge.id:
            logging.info("Deny message sending to edge: %s", edge.id)
            deny_msg = Message()
            deny_msg = deny_msg.deny_merge()
            edge.send_message(deny_msg, self.process_id)
        return

    def handle_merge_mwoe(self, mwoe_msg):
        """Handles the merge mwoe message."""
        for edge in [e for e in self.mst_edges if e != self.parent_edge]:
            edge.send_message(mwoe_msg, self.process_id)
        if mwoe_msg.process != self.process_id:
            logging.info("Returning since MWOE is not self MWOE")
            return
        self.send_join_request(self.get_edge(mwoe_msg.edge_id))
        self.merge_mwoe(mwoe_msg)

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
        for edge in self.mst_edges:
            logging.info("MST EDGE %s", str(edge))
        while len(seen_edges) != len(self.mst_edges):
            for edge in [e for e in self.mst_edges if e not in seen_edges]:
                self.messages_lock.acquire()
                if "ack_leader" in self.messages[edge.id] and\
                        len(self.messages[edge.id]["ack_leader"]):
                    ack_msg = self.messages[edge.id]["ack_leader"].pop()
                    seen_edges.append(edge)
                self.messages_lock.release()
            time.sleep(0.5)
        ack_msg = Message()
        ack_msg = ack_msg.acknowledge_leader()
        in_edge.send_message(ack_msg, self.process_id)
        logging.info("Acknowledge send through edge %s", in_edge.id)
        self.round_completed = True
