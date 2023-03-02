###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# SubscriberMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the subscriber side of things.
#
# Remember that the subscriber middleware does not do anything on its own.
# It must be invoked by the application level logic. This middleware object maintains
# the ZMQ sockets and knows how to talk to Discovery service, etc.
#
# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REQ role to talk to the Discovery service
# and one in the SUB role to receive topic data
# (2) It must, on behalf of the application logic, register the subscriber application with the
# discovery service. To that end, it must use the protobuf-generated serialization code to
# send the appropriate message with the contents to the discovery service.
# (3) On behalf of the subscriber appln,
# it must use the ZMQ setsockopt method to subscribe to all the
# user-supplied topics of interest.
# (4) Since it is a receiver, the middleware object will
# maintain a poller and even loop waiting for some
# subscription to show up (or response from Discovery service).
# (5) On receipt of a subscription, determine which topic it is and let the application level
# handle the incoming data. To that end, you may need to make an upcall to the application-level
# object.
#


# import the needed packages
import csv
from datetime import datetime
import time
from dateutil import parser
import zmq  # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2
import utils
# from CS6381_MW import topic_pb2  # you will need this eventually

# import any other packages you need.

##################################
#       Subscriber Middleware class
##################################


class SubscriberMW ():
    '''Subscriber middleware class'''

    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger  # internal logger for print statements
        self.req = None  # will be a ZMQ REQ socket to talk to Discovery service
        self.sub = None  # will be a ZMQ SUB socket for dissemination
        self.poller = None  # used to wait on incoming replies
        self.addr = None  # our advertised IP address
        self.port = None  # port num where we are going to publish our topics
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True  # in general we keep going thru the event loop

    ########################################
    # configure/initialize
    ########################################
    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("SubscriberMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = args.port
            self.addr = args.addr

            # Next get the ZMQ context
            self.logger.debug("SubscriberMW::configure - obtain ZMQ context")
            #pylint: disable=abstract-class-instantiated
            context = zmq.Context()  # returns a singleton object

            # get the ZMQ poller object
            self.logger.debug("SubscriberMW::configure - obtain the poller")
            self.poller = zmq.Poller()

            # Now acquire the REQ and PUB sockets
            # REQ is needed because we are the client of the Discovery service
            # PUB is needed because we publish topic data
            self.logger.debug(
                "SubscriberMW::configure - obtain REQ and PUB sockets")
            self.req = context.socket(zmq.REQ)
            self.sub = context.socket(zmq.SUB)

            # Since are using the event loop approach, register the REQ socket for incoming events
            # Note that nothing ever will be received on the PUB socket and so it does not make
            # any sense to register it with the poller for an incoming message.
            self.logger.debug(
                "SubscriberMW::configure - register the REQ socket for incoming replies")
            self.poller.register(self.req, zmq.POLLIN)

            # Now connect ourselves to the discovery service. Recall that the IP/port were
            # supplied in our argument parsing. Best practices of ZQM suggest that the
            # one who maintains the REQ socket should do the "connect"
            self.logger.debug(
                "SubscriberMW::configure - connect to Discovery service")
            # For our assignments we will use TCP. The connect string is made up of
            # tcp:// followed by IP addr:port number.
            random_discovery = utils.get_random_disc_node()
            print(random_discovery)
            connect_str = "tcp://" + random_discovery
            self.req.connect(connect_str)

            self.logger.info("SubscriberMW::configure completed")

        except Exception as e:  # pylint: disable=invalid-name
            raise e

    #################################################################
    # run the event loop where we expect to receive a reply to a sent request
    #################################################################
    def event_loop(self, timeout=None):
        ''' Run the event loop '''
        try:
            self.logger.info("SubscriberMW::event_loop - run the event loop")

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
                "SubscriberMW::event_loop - out of the event loop")
        except Exception as e:  # pylint: disable=invalid-name
            raise e

    #################################################################
    # handle an incoming reply
    #################################################################
    def handle_reply(self):
        ''' Handle an incoming reply from the Discovery service '''
        try:
            self.logger.info("SubscriberMW::handle_reply")

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
            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:  # pylint: disable=no-member
                # let the appln level object decide what to do
                timeout = self.upcall_obj.register_response(
                    disc_resp.register_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:  # pylint: disable=no-member
                # this is a response to is ready request
                timeout = self.upcall_obj.lookup_response(
                    disc_resp.lookup_resp)
                print('lookup response', timeout)

            else:  # anything else is unrecognizable by this object
                # raise an exception here
                raise ValueError("Unrecognized response message")

            return timeout

        except Exception as e:  # pylint: disable=invalid-name
            raise e

    ########################################
    # register with the discovery service
    #
    # this method is invoked by application object passing the necessary
    # details but then as a middleware object it is our job to do the
    # serialization using the protobuf generated code
    #
    # No return value from this as it is handled in the invoke_operation
    # method of the application object.
    ########################################
    def register(self, name, topiclist):
        ''' register the appln with the discovery service '''

        try:
            self.logger.info("SubscriberMW::register")

            # as part of registration with the discovery service, we send
            # what role we are playing, the list of topics we are publishing,
            # and our whereabouts, e.g., name, IP and port

            # The following code shows serialization using the protobuf generated code.

            # Build the Registrant Info message first.
            self.logger.debug(
                "SubscriberMW::register - populate the Registrant Info")
            # pylint: disable=no-member
            reg_info = discovery_pb2.RegistrantInfo()  # allocate

            self.logger.debug(
                "SubscriberMW::register - done populating the Registrant Info")

            # Next build a RegisterReq message
            self.logger.debug(
                "SubscriberMW::register - populate the nested register req")
            register_req = discovery_pb2.RegisterReq()  # allocate
            register_req.role = discovery_pb2.ROLE_SUBSCRIBER  # we are a Subscriber
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            # copy contents of inner structure
            register_req.info.CopyFrom(reg_info)
            # this is how repeated entries are added (or use append() or extend ()
            register_req.topiclist[:] = topiclist
            self.logger.debug(
                "SubscriberMW::register - done populating nested RegisterReq")

            # Finally, build the outer layer DiscoveryReq Message
            self.logger.debug(
                "SubscriberMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.register_req.CopyFrom(register_req)
            self.logger.debug(
                "SubscriberMW::register - done building the outer message")

            # now let us stringify the buffer and print it.
            # This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug(
                "Stringified serialized buf = %s", buf2send)

            # now send this to our discovery service
            self.logger.debug(
                "SubscriberMW::register - send stringified buffer to Discovery service")
            # we use the "send" method of ZMQ that sends the bytes
            self.req.send(buf2send)

            # now go to our event loop to receive a response to this request
            self.logger.info(
                "SubscriberMW::register - sent register message and now now wait for reply")

        except Exception as e:  # pylint: disable=invalid-name
            raise e

    ########################################
    # check if the discovery service gives us a green signal to proceed
    #
    # Here we send the isready message and do the serialization
    #
    # No return value from this as it is handled in the invoke_operation
    # method of the application object.
    ########################################
    def lookup(self, topiclist):
        ''' register the appln with the discovery service '''

        try:
            self.logger.info("SubscriberMW::lookup")
            #give some time for system to bootstrap, immediate request will lead to 
            # uncneccessary retries
            time.sleep(5)
            # send lookup req
            self.logger.debug(
                "SubscriberMW::lookup - populate the nested lookup msg")
            # pylint: disable=no-member
            lookup_req = discovery_pb2.LookupPubByTopicReq()  # allocate
            # actually, there is nothing inside that msg declaration.
            self.logger.debug(
                "SubscriberMW::lookup - done populating nested lookup msg")


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
                "SubscriberMW::lookup - send stringified buffer to Discovery service")
            # we use the "send" method of ZMQ that sends the bytes
            self.req.send(buf2send)

            # now go to our event loop to receive a response to this request
            self.logger.info(
                "SubscriberMW::lookup - request sent and now wait for reply")

        except Exception as e:  # pylint: disable=invalid-name
            raise e

    def connect(self,ports, address, topiclist):
        ''' connect to the publisher '''
        print('connecting to publisher')
        print(ports)
        print(address)
        for idx, _ in enumerate(ports):
            self.logger.info("SubscriberMW::connect")
            # connect to the publisher
            print(f'tcp://{address[idx]}:{ports[idx]}')
            self.sub.connect(f'tcp://{address[idx]}:{ports[idx]}')
        for topic in topiclist:
            self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)
        print('connected to publisher')
        cnt = 0
        avg_delay =0
        while True:
            data = self.sub.recv().decode("utf-8")
            print(data)
            self.logger.info(data)
            t1 = data.split('_')[-1]
            t1 = parser.parse(t1)
            t2 = datetime.now()
            seconds = (t2 - t1).total_seconds()
            avg_delay += seconds
            cnt += 1
            if cnt == 100:
                print("average delay is ", avg_delay/cnt)
                
                with open('data.csv', 'a') as csvfile:
                    spamwriter = csv.writer(csvfile, delimiter=' ',
                                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
                    spamwriter.writerow([avg_delay/cnt])
                cnt = 0
                avg_delay = 0
            print("delay is ", seconds)

        return
    # set upcall handle
    #
    # here we save a pointer (handle) to the application object
    ########################################

    def set_upcall_handle(self, upcall_obj):
        ''' set upcall handle '''
        self.upcall_obj = upcall_obj

    ########################################
    # disable event loop
    #
    # here we just make the variable go false so that when the event loop
    # is running, the while condition will fail and the event loop will terminate.
    ########################################
    def disable_event_loop(self):
        ''' disable event loop '''
        self.handle_events = False
