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


import json
import time   # for sleep
import zmq  # ZMQ sockets

# import serialization logic

from CS6381_MW import discovery_pb2, leader_election
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
        self.logger = logger  # internal logger for print statements
        self.req = None  # will be a ZMQ REQ socket to talk to Discovery service
        self.pub = None  # will be a ZMQ PUB socket for dissemination
        self.poller = None  # used to wait on incoming replies
        self.addr = None  # our advertised IP address
        self.port = None  # port num where we are going to publish our topics
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True  # in general we keep going thru the event loop
        self.rep = None
        self.req = None
        self.is_leader = False
        self.pub = None
        self.was_leader = None

    ########################################
    # configure/initialize
    ########################################
    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("DiscoveryMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = args.port
            self.addr = args.addr
            self.name = args.name
            # Next get the ZMQ context
            self.logger.debug("DiscoveryMW::configure - obtain ZMQ context")
            # pylint: disable=abstract-class-instantiated
            context = zmq.Context()  # returns a singleton object

            # get the ZMQ poller object
            self.logger.debug("DiscoveryMW::configure - obtain the poller")
            self.poller = zmq.Poller()

            # Now acquire the REQ and PUB sockets
            # REQ is needed because we are the client of the Discovery service
            # PUB is needed because we publish topic data
            self.logger.info("DiscoveryMW::configure - obtain REP socket")
            self.rep = context.socket(zmq.REP)
            self.pub = context.socket(zmq.PUB)

            with open('CS6381_MW/discovery_pubs.json') as user_file:
                file_contents = user_file.read()

            discovery_nodes = json.loads(file_contents)

            bind_string = "tcp://*:" + \
                str(discovery_nodes[self.name].split(":")[1])
            print(bind_string)
            self.pub.bind(bind_string)
            self.reqs = []

            self.logger.info(
                "PublisherMW::configure - register the REQ socket for incoming replies")
            self.poller.register(self.rep, zmq.POLLIN)

            # Since are using the event loop approach, register the REQ socket for incoming events
            # Note that nothing ever will be received on the PUB socket and so it does not make
            # any sense to register it with the poller for an incoming message.
            #   self.logger.debug ("DiscoveryMW::configure - register the REQ socket
            #   for incoming replies")
            #   self.poller.register (self.req, zmq.POLLIN)

            # Now connect ourselves to the discovery service. Recall that the IP/port were
            # supplied in our argument parsing. Best practices of ZQM suggest that the
            # one who maintains the REQ socket should do the "connect"
            self.logger.info(
                "DiscoveryMW::configure - connect to Discovery service")
            # For our assignments we will use TCP. The connect string is made up of
            # tcp:// followed by IP addr:port number.
            connect_str = "tcp://*:" + str(args.port)
            self.rep.bind(connect_str)
            with open('CS6381_MW/discovery_nodes.json') as user_file:
                file_contents = user_file.read()

            discovery_nodes = json.loads(file_contents)

            for value in discovery_nodes.values():
                self.logger.info('connecting to other discovery nodes')
                socket = context.socket(zmq.REQ)
                if value == args.addr + ":" + str(args.port):
                    continue
                connect_str = "tcp://" + value
                print(connect_str)
                socket.setsockopt(zmq.RCVTIMEO, 3000)
                socket.setsockopt(zmq.LINGER, 0)
                socket.setsockopt(zmq.REQ_RELAXED, 1)
                socket.connect(connect_str)
                self.reqs.append(socket)
                self.poller.register(socket, zmq.POLLIN)

            self.logger.info("activating zookeeper")
            zookeeper_obj = leader_election.ApplicationNode(self, server_name=self.name,
                                                            server_data=self.addr +
                                                            ":"+str(self.port),
                                                            chroot="/application", zookeeper_hosts="localhost:2181")

            self.logger.info("DiscoveryMW::configure completed")

        except Exception as e_exception:
            raise e_exception

    def recv_messages(self):
        """Function to receive messages from the pubs/subs/brokers."""

        while True:
            self.logger.info("inside recv_messages")
            events = dict(self.poller.poll())
            incoming_request = False

            if self.rep in events:

                #  Wait for next request from client
                print('Waiting for request...')
                message = self.rep.recv()
                print(f"Received request: {message}")
                try:
                    broker_data = message.decode('ascii')
                    if broker_data.startswith('broker:'):
                        print(broker_data[7:])
                        self.upcall_obj.update_broker(broker_data[7:])
                        self.rep.send_string("OK")
                        continue

                except: # pylint: disable=bare-except
                    pass

                try:
                    self.internal_discovery_msg(message)
                    incoming_request = False
                    continue
                except:  # pylint: disable=bare-except
                    pass
                # Decode message and print

                register_msg = discovery_pb2.DiscoveryReq()  # pylint: disable=no-member
                register_msg.ParseFromString(message)
                if register_msg.msg_type == 1:
                    if register_msg.register_req.role == 1:
                        incoming_request = True
                    if register_msg.register_req.role == 3:
                        self.logger.info('inform others that broker has changed')
                        self.pub.send_string("broker:" + register_msg.register_req.info.addr+":" +
                             str(register_msg.register_req.info.port))
                        self.sync_broker_state(register_msg.register_req.info.addr+":" + 
                                str(register_msg.register_req.info.port))
                        # new publisher in the system
                print(f"Decoded message: {register_msg}")
                resp_to_send = self.upcall_obj.handle_messages(register_msg)

                print(resp_to_send)
                buf2send = resp_to_send.SerializeToString()
                #  Do some 'work'
                time.sleep(1)

                #  Send reply back to client
                self.rep.send(buf2send)

            if incoming_request:
                self.sync_state(register_msg)

    def internal_discovery_msg(self, message):
        '''Function to handle internal discovery messages'''
        my_json = message.decode('utf8').replace("'", '"')

        self.logger.info('- ' * 20)

        # Load the JSON to a Python list & dump it back out as formatted JSON
        data = json.loads(my_json)
        data = json.loads(data)
        self.upcall_obj.update_publisher_data(data)
        self.rep.send_string('ack')
        self.logger.info('ack sent')

    def sync_state(self, register_msg):
        '''Function to sync state with other discovery nodes'''
        # send messages to other discovery followers
        # with updated sync state
        if register_msg.register_req.role != 1:
            return
        publishers = self.upcall_obj.get_publishers()
        self.logger.info(publishers)

        for i in publishers:
            publishers[i]['topics'] = list(publishers[i]['topics'])

        for req in self.reqs:
            req.send_json(json.dumps(publishers))
            self.logger.info('sending message to other discovery nodes')
            try:
                response = req.recv()
                self.logger.info(response)
            except: # pylint: disable=bare-except
                self.logger.info('no response from other discovery node')
                continue
        print('new publisher added to the system')
        self.pub.send_string("pub:" + register_msg.register_req.info.addr+":" +
                             str(register_msg.register_req.info.port))

    def sync_broker_state(self, broker_addr):
        '''Function to sync broker state with other discovery nodes'''
        # send messages to other discovery followers
        # with updated sync state
        for req in self.reqs:
            req.send_string("broker:" + broker_addr)
            self.logger.info('sending message to other discovery nodes')
            try:
                response = req.recv()
                self.logger.info(response)
            except: # pylint: disable=bare-except
                pass

    def set_leader(self):
        ''' Set the leader flag'''
        self.is_leader = True
        if self.port != 5555:
            print("sending leader message")
            self.pub.send_string("leader:" + self.addr+":"+str(self.port))
        with open('CS6381_MW/leaders.json') as user_file:
            leader_contents = user_file.read()
        leader_nodes = json.loads(leader_contents)
        leader_nodes['discovery'] = self.addr+":"+str(self.port)
        with open('CS6381_MW/leaders.json', 'w') as user_file:
            json.dump(leader_nodes, user_file)

    def set_upcall_handle(self, upcall_obj):
        ''' Set the upcall object handle '''
        self.upcall_obj = upcall_obj
