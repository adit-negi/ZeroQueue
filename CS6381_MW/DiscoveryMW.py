###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the discovery middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student.
#
# The discovery service is a server. So at the middleware level, we will maintain
# a REP socket binding it to the port on which we expect to receive requests.
#
# There will be a forever event loop waiting for requests. Each request will be parsed
# and the application logic asked to handle the request. To that end, an upcall will need
# to be made to the application logic.


import hashlib
import time   # for sleep
import zmq  # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2
from CS6381_MW.Common import Chord
from utils import find_node_hash
# from CS6381_MW import topic_pb2  # you will need this eventually

# import any other packages you need.

##################################
#       Discovery Middleware class
##################################


class DiscoveryMW ():
    """A dummy docstring."""

    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger  # internal logger for self.logger.info statements
        self.req = None  # will be a ZMQ REQ socket to talk to Discovery service
        self.pub = None  # will be a ZMQ PUB socket for dissemination
        self.poller = None  # used to wait on incoming replies
        self.addr = None  # our advertised IP address
        self.port = None  # port num where we are going to publish our topics
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True  # in general we keep going thru the event loop
        self.rep = None
        self.bits_hash = 16
        self.topics_hash = []
        self.finger_table = None
        self.predecessor = None
        self.curr_node_hash = None
        self.finger_sockets = None
        self.dht_data = None
        self.register_sock = None
        self.ready = False
        self.is_register_sock = False

    ########################################
    # configure/initialize
    ########################################
    def configure(self, args, finger_table, dht_data):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("DiscoveryMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = args.port
            self.addr = args.addr

            # Next get the ZMQ context
            self.logger.debug("DiscoveryMW::configure - obtain ZMQ context")
            # pylint: disable=abstract-class-instantiated
            context = zmq.Context()  # returns a singleton object
            self.dht_data = dht_data
            # get the ZMQ poller object
            self.logger.debug("DiscoveryMW::configure - obtain the poller")
            self.poller = zmq.Poller()

            # Now acquire the REQ and PUB sockets
            # REQ is needed because we are the client of the Discovery service
            # PUB is needed because we publish topic data
            self.logger.debug("DiscoveryMW::configure - obtain REP socket")
            self.rep = context.socket(zmq.REP)

            # Since are using the event loop approach, register the REQ socket for incoming events
            # Note that nothing ever will be received on the PUB socket and so it does not make
            # any sense to register it with the poller for an incoming message.
            #   self.logger.debug ("DiscoveryMW::configure - register the REQ socket
            #   for incoming replies")
            #   self.poller.register (self.req, zmq.POLLIN)

            # Now connect ourselves to the discovery service. Recall that the IP/port were
            # supplied in our argument parsing. Best practices of ZQM suggest that the
            # one who maintains the REQ socket should do the "connect"
            self.logger.debug(
                "DiscoveryMW::configure - connect to Discovery service")
            # For our assignments we will use TCP. The connect string is made up of
            # tcp:// followed by IP addr:port number.
            connect_str = "tcp://*:" + str(args.port)
            self.rep.bind(connect_str)
            self.curr_node_hash = self.find_curr_node_hash(dht_data, args)
            # req sockets for all the fingers in the finger table.
            curr_dht_finger_table = finger_table[args.name]
            fingers = {}
            self.finger_table = curr_dht_finger_table
            self.predecessor = self.finger_table['predecessor']
            for i in curr_dht_finger_table:
                if i == 'predecessor':  # skip predecessor
                    continue
                if curr_dht_finger_table[i]["id"] not in fingers:
                    curr_req = context.socket(zmq.REQ)
                    # find the ip and port of the finger from dht_data
                    ip, port = self.get_dht_ip_port(
                        curr_dht_finger_table[i]["id"])
                    
                    connect_str = "tcp://" + ip + ":" + str(port)
                    self.logger.info(connect_str)
                    curr_req.setsockopt(zmq.RCVTIMEO, 320000)
                    curr_req.setsockopt(zmq.LINGER, 0)
                    curr_req.setsockopt(zmq.REQ_RELAXED,1)
                    curr_req.connect(connect_str)
                    
                    fingers[curr_dht_finger_table[i]["id"]] = {
                        "socket": curr_req,
                        "extra": {}
                    }
                    self.logger.info(fingers)

            self.register_sock = context.socket(zmq.REQ)
            connect_str  = self.node_handling_register_hash()
            self.logger.info(connect_str)
            self.logger.info(args.addr + ":" + str(args.port))
            if "tcp://"+args.addr + ":" + str(args.port) == connect_str:
                self.is_register_sock = True
            self.register_sock.setsockopt(zmq.RCVTIMEO, 10000)
            self.register_sock.setsockopt(zmq.LINGER, 0)
            self.register_sock.setsockopt(zmq.REQ_RELAXED,1)
            self.register_sock.connect(connect_str)
            
            self.finger_sockets = fingers
            self.logger.info("DiscoveryMW::configure completed")

        except Exception as e_exception:
            raise e_exception

    def recv_messages(self):
        """Function to receive messages from the pubs/subs/brokers."""
        while True:
            #  Wait for next request from client
            self.logger.info('Waiting for request...')
            message = self.rep.recv()
            if message == b'register':
                self.logger.info("RIGHT HERE")
                self.upcall_obj.update_register_counter()
                self.rep.send(b'OK')
                continue
            if message == b'is_ready':
                flag = self.upcall_obj.is_ready_flag()
                if flag:
                    self.rep.send(b'OK')
                else:
                    self.rep.send(b'NOT')
                continue
            self.logger.info(f"Received request: {message}")
            # Decode message and self.logger.info
            discovery_req_msg = discovery_pb2.DiscoveryReq()  # pylint: disable=no-member
            discovery_req_msg.ParseFromString(message)
            self.logger.info(discovery_req_msg.msg_type)
            # check type of message and apply chord algorithm to it
            if discovery_req_msg.msg_type == 1:
                # register message
                self.logger.info("Register message")
                self.handle_register_message(discovery_req_msg)
            elif discovery_req_msg.msg_type == 5:
                self.logger.info("regsiter message forwared from another node")
                self.logger.info(discovery_req_msg)
                self.handle_discovery_req_message(discovery_req_msg)
            elif discovery_req_msg.msg_type == 6:
                self.handle_discovery_lookup_message(discovery_req_msg)
            elif discovery_req_msg.msg_type == 2:
                self.logger.info('is ready msg')
                self.logger.info(discovery_req_msg)
                if self.is_register_sock:
                    self.ready = self.upcall_obj.is_ready_flag()
                else:
                    self.register_sock.send_string("is_ready")
                    try:
                        resp = self.register_sock.recv_string()
                    except:
                        resp = None
                        self.logger.info("request timed out")
                    if resp == 'OK':
                        self.ready = True
                    if not resp:
                        self.logger.info("request timed out")
            self.logger.info(f"Decoded message: {discovery_req_msg}")
            resp_to_send = self.upcall_obj.handle_messages(discovery_req_msg)

            self.logger.info(resp_to_send)
            buf2send = resp_to_send.SerializeToString()
            #  Do some 'work'
            time.sleep(1)

            #  Send reply back to client
            self.rep.send(buf2send)

    def handle_register_message(self, discovery_req_msg):
        ''' Handle the register message '''
        self.logger.debug("DiscoveryMW::handle_register_message")
        if discovery_req_msg.register_req.role == 2:
            self.logger.info("Register message for subscriber")
        else:
            self.logger.info("Register message for publisher -- handle topics")
            topiclist = discovery_req_msg.register_req.topiclist
            
            
            for topic in topiclist:
                self.topics_hash.append((topic, self.hash_func(topic)))

            # for each topic list, send a register message to the appropriate finger
            for topic, hash_topic in self.topics_hash:
                self.logger.info(topic)
                self.logger.info(hash_topic)
                # curr_node_handle call
                self.curr_node_handle_register(hash_topic, topic,
                                            discovery_req_msg.register_req.info.addr, 
                                            discovery_req_msg.register_req.info.port,
                                            discovery_req_msg.register_req.role)

        if self.is_register_sock:
            self.upcall_obj.update_register_counter()
            self.logger.info('updating register counter because I am the register node')
        else:
            self.logger.info("SENT MESSAGE TO REGISTER")
            self.register_sock.send_string("register")
            resp = None
            try:
                resp = self.register_sock.recv_string()
            except:
                resp = None
                self.logger.info("request timed out")
            self.logger.info(resp)
            while not resp:
                self.logger.info('timeout')
                self.logger.info('didnt get response breaking out')
                for sleep in [5,10,15,20, 25]:
                    time.sleep(sleep)
                    self.logger.info("trying again")
                    self.register_sock.send_string("register")
                    resp = None
                    try:
                        resp = self.register_sock.recv_string()
                    except:
                        resp = None
                        self.logger.info("request timed out")
                    if resp == 'OK':
                        self.logger.info('got response')
                        break
                if resp:
                    break
    def handle_discovery_req_message(self, discovery_req_msg):
        ''' Handle the discovery request message '''
        topic = discovery_req_msg.disc_reg_req.topic
        hash_topic = discovery_req_msg.disc_reg_req.hashedtopic
        ip = discovery_req_msg.disc_reg_req.ip
        port = discovery_req_msg.disc_reg_req.port
        pubname = discovery_req_msg.disc_reg_req.pubname

        self.curr_node_handle_register(hash_topic, topic, ip, port, pubname)

    def handle_discovery_lookup_message(self, discovery_req_msg):
        ''' Handle the discovery request message '''
        topic = discovery_req_msg.disc_reg_req.topic
        hash_topic = discovery_req_msg.disc_reg_req.hashedtopic

        self.curr_node_handle_lookup(hash_topic, topic)

    def curr_node_handle_register(self, hash_topic, topic, ip, port, role):
        ''' Handle the register message for the current node '''
        self.logger.info("DiscoveryMW::curr_node_handle_register")
        self.curr_node_hash = int(self.curr_node_hash)
        self.predecessor = int(self.predecessor)
        hash_topic = int(hash_topic)

        if self.predecessor < self.curr_node_hash:
            self.logger.info(hash_topic, self.curr_node_hash)
            if hash_topic > self.predecessor and hash_topic <= self.curr_node_hash:
                # current node can handle this topic
                self.logger.info(
                    'self.logger.infoing ip prot'
                )
                self.logger.info(ip)
                self.logger.info(port)
                self.upcall_obj.handle_register_message(
                    topic, hash_topic, ip, port, role)
            else:
                # send regsiter request to appropriate finger
                self.logger.info('sendng to finger')
                self.send_to_finger(hash_topic, topic, ip, port, role)
        else:
            # this is the edge case when predecessor is largest node in the ring
            if hash_topic > self.predecessor:
                # current node can handle this topic
                self.upcall_obj.handle_register_message(
                    topic, hash_topic, ip, port, role)
            else:
                # send regsiter request to appropriate finger
                self.logger.info('sendng to finger2')
                self.send_to_finger(hash_topic, topic, ip, port,role)

    def curr_node_handle_lookup(self, hash_topic, topic):
        ''' Handle the register message for the current node '''
        self.logger.info("DiscoveryMW::curr_node_handle_register")
        self.curr_node_hash = int(self.curr_node_hash)
        self.predecessor = int(self.predecessor)
        hash_topic = int(hash_topic)
        pubs_by_topic = self.upcall_obj.get_pubs_by_topic(topic)
        if pubs_by_topic:
            return
        if self.predecessor < self.curr_node_hash:
            if hash_topic > self.predecessor and hash_topic <= self.curr_node_hash:
                # current node can handle this topic
                self.upcall_obj.handle_lookup_message(
                    hash_topic, topic)
            else:
                # send regsiter request to appropriate finger
                self.logger.info('sendng to finger')
                self.send_to_finger(hash_topic, topic, None, None, None, True)
        else:
            # this is the edge case when predecessor is largest node in the ring
            if hash_topic > self.predecessor:
                # current node can handle this topic
                self.upcall_obj.handle_lookup_message(
                        hash_topic, topic)
            else:
                # send regsiter request to appropriate finger
                self.logger.info('sendng to finger2')
                self.send_to_finger(hash_topic, topic, None, None, None, True)

    def send_to_finger(self, hash_topic, topic, ip, port, role, lookup=False):
        ''' Send the register message to the finger '''

        chord = Chord()
        self.logger.info('chord initialized')
        node_value = chord.chord_algo(hash_topic, self.finger_table, self.curr_node_hash)

        socket = self.finger_sockets[node_value]["socket"]

        if not ip and not port:
            disc_req = self.construct_discover_lookup_msg(topic, hash_topic)
        else:
            disc_req = self.construct_discover_register_msg(node_value, topic,
                                                        hash_topic, ip, port, role)
        self.logger.info(socket)
        self.logger.info('seding to ' )
        self.logger.info(node_value)
        socket.send(disc_req.SerializeToString())
        self.logger.info('sent now waiting for response')
        # wait for response
        response = None
        try:
            response = socket.recv(zmq.NOBLOCK)
        except:
            self.logger.info('timeout')
            print('didnt get response breaking out')
        if not response:
            self.logger.info('timeout')
            print('didnt get response breaking out')
        # TODO: handle the response
        # TODO: if this fails raise an exception
        if lookup and response:
            '''deconde tthe response'''
            disc_resp = discovery_pb2.DiscoveryResp()  # pylint: disable=no-member
            disc_resp.ParseFromString(response)
            self.logger.info(disc_req)
            #set state in the upcall object
            self.upcall_obj.handle_lookup_resposne(disc_resp, disc_req.disc_reg_req.topic)
            self.logger.info(disc_resp)
            self.logger.info(type(response))
        self.logger.info(response)

    def construct_discover_register_msg(self, node_value, topic, topic_hash, ip, port, role):
        ''' Construct the discovery register message '''
        self.logger.debug("DiscoveryMW::construct_discover_register_msg")
        port = str(port)
        topic_hash = str(topic_hash)
        # create the discovery register message
        discovery_register_msg = discovery_pb2.DiscoveryRegisterReq()  # pylint: disable=no-member
        discovery_register_msg.ip = ip
        discovery_register_msg.port = port
        discovery_register_msg.topic = topic
        discovery_register_msg.hashedtopic = topic_hash
        discovery_register_msg.pubname = str(role)

        disc_req = discovery_pb2.DiscoveryReq()  # pylint: disable=no-member
        disc_req.msg_type = discovery_pb2.TYPE_DISC_REG  # pylint: disable=no-member
        # It was observed that we cannot directly assign the nested field here.
        # A way around is to use the CopyFrom method as shown
        disc_req.disc_reg_req.CopyFrom(discovery_register_msg)
        return disc_req

    def construct_discover_lookup_msg(self, topic, topic_hash):
        ''' Construct the discovery lookup message '''
        self.logger.info("DiscoveryMW::construct_discover_lookup_msg")

        topic_hash = str(topic_hash)
        # create the discovery register message
        discovery_register_msg = discovery_pb2.DiscoveryRegisterReq()  # pylint: disable=no-member


        discovery_register_msg.topic = topic
        discovery_register_msg.hashedtopic = topic_hash


        disc_req = discovery_pb2.DiscoveryReq()  # pylint: disable=no-member
        disc_req.msg_type = discovery_pb2.TYPE_DISC_LOOKUP  # pylint: disable=no-member
        # It was observed that we cannot directly assign the nested field here.
        # A way around is to use the CopyFrom method as shown
        disc_req.disc_reg_req.CopyFrom(discovery_register_msg)
        return disc_req

    def set_upcall_handle(self, upcall_obj):
        ''' Set the upcall object handle '''
        self.upcall_obj = upcall_obj

    def get_dht_ip_port(self, node_id):
        ''' Get the ip and port of the DHT '''
        for node in self.dht_data['dht']:
            if node['id'] == node_id:
                return node['IP'], node['port']
        return None, None

    def hash_func(self, id):
        '''convert messages to there respective hash values'''
        self.logger.debug("ExperimentGenerator::hash_func")

        # first get the digest from hashlib and then take the desired number of bytes from the
        # lower end of the 256 bits hash. Big or little endian does not matter.
        # this is how we get the digest or hash value
        hash_digest = hashlib.sha256(bytes(id, "utf-8")).digest()
        # figure out how many bytes to retrieve
        # otherwise we get float which we cannot use below
        num_bytes = int(self.bits_hash/8)
        # take lower N number of bytes
        hash_val = int.from_bytes(hash_digest[:num_bytes], "big")

        return hash_val

    def find_curr_node_hash(self, dht_data, args):
        ''' Find the hash of the current node '''
        for node in dht_data['dht']:
            if node['id'] == args.name:
                return node['hash']
        return None

    def node_handling_register_hash(self):
        ''' Handle the register hash ''' 
        self.logger.info("DiscoveryMW::node_handling_register_hash")
        # get the hash of the current node
        register_hash = self.hash_func('register')
        self.logger.info(register_hash)
        # find node which can handle this hash
        node_id, ip, port = find_node_hash(register_hash)
        self.logger.info(ip)
        self.logger.info(node_id)
        return "tcp://" + ip + ":" + str(port)

    def system_ready(self):
        ''' Check if the system is ready '''
        return self.ready

    def get_system_ready_update(self):
        ''' Get the system ready update '''
        if self.ready: 
            return self.ready
        self.register_sock.send_string("is_ready")
        
        resp = None
        try:
            resp = self.register_sock.recv_string()
        except:
            self.logger.info('timeout')
            print('didnt get response breaking out')
        if resp == 'OK':
            self.ready = True
        if not resp:
            self.logger.info('timeout')
            print('didnt get response breaking out')
        return self.ready