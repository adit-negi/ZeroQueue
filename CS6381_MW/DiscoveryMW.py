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


import time   # for sleep
import zmq  # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2

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

            # Next get the ZMQ context
            self.logger.debug("DiscoveryMW::configure - obtain ZMQ context")
            context = zmq.Context()  # returns a singleton object

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

            self.logger.info("DiscoveryMW::configure completed")

        except Exception as e_exception:
            raise e_exception

    def recv_messages(self):
        """Function to receive messages from the pubs/subs/brokers."""
        while True:
            #  Wait for next request from client
            print('Waiting for request...')
            message = self.rep.recv()
            print(f"Received request: {message}")
            # Decode message and print
            # TODO: Add logic to handle different types of messages
            register_msg = discovery_pb2.DiscoveryReq()  # pylint: disable=no-member
            register_msg.ParseFromString(message)

            print(f"Decoded message: {register_msg}")
            resp_to_send = self.upcall_obj.handle_messages(register_msg)
            buf2send = resp_to_send.SerializeToString()
            #  Do some 'work'
            time.sleep(1)

            #  Send reply back to client
            self.rep.send(buf2send)

    def set_upcall_handle(self, upcall_obj):
        ''' Set the upcall object handle '''
        self.upcall_obj = upcall_obj
