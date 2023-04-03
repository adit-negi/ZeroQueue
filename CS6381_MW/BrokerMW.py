###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# BrokerMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# Broker and Broker roles. So in addition to the REQ socket to talk to the
# Broker service, it will have both PUB and SUB sockets as it must work on
# behalf of the real Brokers and Brokers. So this will have the logic of
# both Broker and Broker middleware.

import json
import time   # for sleep
import zmq  # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2, leader_election

# from CS6381_MW import topic_pb2  # you will need this eventually

# import any other packages you need.

##################################
#       Broker Middleware class
##################################



class BrokerMW ():
    """A dummy docstring."""

    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger  # internal logger for print statements
        self.req = None  # will be a ZMQ REQ socket to talk to Broker service
        self.pub = None  # will be a ZMQ PUB socket for dissemination
        self.poller = None  # used to wait on incoming replies
        self.addr = None  # our advertised IP address
        self.port = None  # port num where we are going to publish our topics
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True  # in general we keep going thru the event loop
        self.rep = None
        self.sub = None
        self.topiclist = []
        self.leader = False

    ########################################
    # configure/initialize
    ########################################
    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("BrokerMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = args.port
            self.addr = args.addr
            self.name = args.name
            # Next get the ZMQ context
            self.logger.debug("BrokerMW::configure - obtain ZMQ context")
            #pylint: disable=abstract-class-instantiated
            context = zmq.Context()  # returns a singleton object

            # get the ZMQ poller object
            self.logger.debug("BrokerMW::configure - obtain the poller")
            self.poller = zmq.Poller()

            # Now acquire the REQ and PUB sockets
            # REQ is needed because we are the client of the Broker service
            # PUB is needed because we publish topic data
            self.logger.debug("BrokerMW::configure - obtain REQ socket")
            self.req = context.socket(zmq.REQ)
            self.sub = context.socket(zmq.SUB)
            with open('CS6381_MW/discovery_pubs.json') as user_file:
                file_contents = user_file.read()

            discovery_nodes = json.loads(file_contents)            
            for value in discovery_nodes.values():
                self.logger.info('connecting to other discovery nodes')
                self.sub.connect(f'tcp://{value}')
            self.sub.setsockopt_string(zmq.SUBSCRIBE, 'pub')

            self.logger.debug(
                "BrokerMW::configure - register the REQ socket for incoming replies")
            self.poller.register(self.req, zmq.POLLIN)
            # Since are using the event loop approach, register the REQ socket for incoming events
            # Note that nothing ever will be received on the PUB socket and so it does not make
            # any sense to register it with the poller for an incoming message.
            #   self.logger.debug ("BrokerMW::configure - register the REQ socket
            #   for incoming replies")
            #   self.poller.register (self.req, zmq.POLLIN)

            # Now connect ourselves to the Broker service. Recall that the IP/port were
            # supplied in our argument parsing. Best practices of ZQM suggest that the
            # one who maintains the REQ socket should do the "connect"
            self.logger.debug(
                "BrokerMW::configure - connect to Broker service")
            # For our assignments we will use TCP. The connect string is made up of
            # tcp:// followed by IP addr:port number.
            connect_str = "tcp://" + args.discovery
            self.req.connect(connect_str)
            self.pub = context.socket(zmq.PUB)
            bind_string = "tcp://*:" + str(self.port)
            self.pub.bind(bind_string)
            self.topiclist = ["weather", "humidity", "airquality", "light",
                            "pressure", "temperature", "sound", "altitude",
                            "location"]

            self.logger.info("BrokerMW::configure - leader election")
            leader_election.ApplicationNode(self, server_name=self.name,
                                                            server_data=self.addr +
                                                            ":"+str(self.port),
                                                            chroot="/broker", zookeeper_hosts="localhost:2181")
            self.logger.info("BrokerMW::configure completed")
            while not self.leader:
                # wait for leader election to complete
                # broker design is stateless so we replicas can just wait
                time.sleep(1)

        except Exception as e_exception:
            raise e_exception

    def register(self):
        ''' register the appln with the discovery service '''

        try:
            self.logger.info("BrokerMW::register")

            # as part of registration with the discovery service, we send
            # what role we are playing, the list of topics we are publishing,
            # and our whereabouts, e.g., name, IP and port

            # The following code shows serialization using the protobuf generated code.

            # Build the Registrant Info message first.
            self.logger.debug(
                "BrokerMW::register - populate the Registrant Info")
            #pylint: disable=no-member
            reg_info = discovery_pb2.RegistrantInfo()  # allocate
            reg_info.id = "broker"  # our id
            reg_info.addr = self.addr  # our advertised IP addr where we are publishing
            reg_info.port = self.port  # port on which we are publishing
            self.logger.debug(
                "BrokerMW::register - done populating the Registrant Info")

            # Next build a RegisterReq message
            self.logger.debug(
                "BrokerMW::register - populate the nested register req")
            register_req = discovery_pb2.RegisterReq()  # allocate
            register_req.role = discovery_pb2.ROLE_BOTH  # we are a Broker
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            # copy contents of inner structure
            register_req.info.CopyFrom(reg_info)
            # this is how repeated entries are added (or use append() or extend ()
            register_req.topiclist[:] = []
            self.logger.debug(
                "BrokerMW::register - done populating nested RegisterReq")

            # Finally, build the outer layer DiscoveryReq Message
            self.logger.debug(
                "BrokerMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.register_req.CopyFrom(register_req)
            self.logger.debug(
                "BrokerMW::register - done building the outer message")

            # now let us stringify the buffer and print it.
            # This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug(
                "Stringified serialized buf = %s", buf2send)

            # now send this to our discovery service
            self.logger.debug(
                "BrokerMW::register - send stringified buffer to Discovery service")
            # we use the "send" method of ZMQ that sends the bytes
            self.req.send(buf2send)

            # now go to our event loop to receive a response to this request
            self.logger.info(
                "BrokerMW::register - sent register message and now now wait for reply")

        except Exception as e: #pylint: disable=invalid-name
            raise e


    def lookup(self, topiclist):
        """Function to lookup from discovery by the broker service."""
        #  Send request, get reply
        self.logger.info("Sending request to Broker service...")
        try:
            self.logger.info("BrokerMW::lookup")

            # send lookup req
            self.logger.debug(
                "BrokerMW::lookup - populate the nested lookup msg")
            # pylint: disable=no-member
            lookup_req = discovery_pb2.LookupPubByTopicReq()  # allocate
            # actually, there is nothing inside that msg declaration.
            self.logger.debug(
                "BrokerMW::lookup - done populating nested lookup msg")


            lookup_req.topiclist[:] = topiclist
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown

            disc_req = discovery_pb2.DiscoveryReq()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.lookup_req.CopyFrom(lookup_req)
            # now let us stringify the buffer and print it.
            # This is actually a sequence of bytes and not a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug(
                "Stringified serialized buf = %s", buf2send)

            # now send this to our discovery service
            self.logger.debug(
                "BrokerMW::lookup - send stringified buffer to Discovery service")
            # we use the "send" method of ZMQ that sends the bytes
            self.req.send(buf2send)

            # now go to our event loop to receive a response to this request
            self.logger.info(
                "BrokerMW::lookup - request sent and now wait for reply")
            time.sleep(1)

        except Exception as e:  # pylint: disable=invalid-name
            raise e


    def dissimenate(self, ports, address, topiclist):
        """dissemante data via broker"""
        self.logger.info("Connect to all the Brokers...")
        print('connecting to Broker')
        print(ports)
        print(address)
        for idx, _ in enumerate(ports):
            self.logger.info("BrokerMW::connect")
            # connect to the Broker
            print(f'tcp://{address[idx]}:{ports[idx]}')
            self.sub.connect(f'tcp://{address[idx]}:{ports[idx]}')
        for topic in self.topiclist:
            self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)
        
        print('connected to Broker')
        while True:
            data_recv = self.sub.recv()
            self.logger.info("BrokerMW::dissimenate")
            self.logger.info(data_recv)
            data = data_recv.decode('ascii')
            if len(data)>4 and data[:4] == 'pub:':
                self.logger.info(data[4:])
                self.sub.connect(f'tcp://{data[4:]}')
                continue
            self.pub.send(data_recv)


    def handle_reply(self):
        ''' Handle an incoming reply from the Discovery service '''
        try:
            self.logger.info("BrokerMW::handle_reply")

            # let us first receive all the bytes
            bytes_rcvd = self.req.recv()
            print(bytes_rcvd)
            # now use protobuf to deserialize the bytes
            # The way to do this is to first allocate the space for the
            # message we expect, here DiscoveryResp and then parse
            # the incoming bytes and populate this structure (via protobuf code)
            disc_resp = discovery_pb2.DiscoveryResp()  # pylint: disable=no-member
            disc_resp.ParseFromString(bytes_rcvd)
            # demultiplex the message based on the message type but let the application
            # object handle the contents as it is best positioned to do so. See how we make
            # the upcall on the application object by using the saved handle to the appln object.
            #
            # Note also that we expect the return value to be the desired timeout to use
            # in the next iteration of the poll.
            if disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:  # pylint: disable=no-member
                # this is a response to is ready request
                timeout = self.upcall_obj.lookup_response(
                    disc_resp.lookup_resp)
                print('lookup response', timeout)

            elif disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:  # pylint: disable=no-member
                # this is a response to register request
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
                print('register response', timeout)

            else:  # anything else is unrecognizable by this object
                # raise an exception here
                raise ValueError("Unrecognized response message")

            return timeout

        except Exception as e:  # pylint: disable=invalid-name
            raise e

    def set_upcall_handle(self, upcall_obj):
        ''' Set the upcall object handle '''
        self.upcall_obj = upcall_obj

    def event_loop(self, timeout=None):
        ''' Run the event loop '''
        try:
            self.logger.info("BrokerMW::event_loop - run the event loop")

            # we are using a class variable called "handle_events" which is set to
            # True but can be set out of band to False in order to exit this forever
            # loop
            while self.handle_events:  # it starts with a True value
                # poll for events. We give it an infinite timeout.
                # The return value is a socket to event mask mapping
                events = dict(self.poller.poll(timeout=timeout))

                # Unlike the previous starter code, here we are never returning from
                # the event loop but handle everything in the same locus of control
                # Notice, also that after handling the event, we retrieve a new value
                # for timeout which is used in the next iteration of the poll

                # check if a timeout has occurred. We know this is the case when
                # the event mask is empty
                if not events:
                    # timeout has occurred so it is time for us to make appln-level
                    # method invocation. Make an upcall to the generic "invoke_operation"
                    # which takes action depending on what state the application
                    # object is in.
                    timeout = self.upcall_obj.invoke_operation()
                # this is the only socket on which we should be receiving replies
                elif self.req in events:

                    # handle the incoming reply from remote entity and return the result
                    timeout = self.handle_reply()

                else:
                    raise Exception("Unknown event after poll")

            self.logger.info(
                "BrokerMW::event_loop - out of the event loop")
        except Exception as e:  # pylint: disable=invalid-name
            raise e

    def set_leader(self):
        ''' Set the leader flag '''
        self.leader = True
        print('sending new broker')
        print('broker:'+self.addr+":"+str(self.port))
        self.pub.send_string("broker:"+self.addr+":"+str(self.port))