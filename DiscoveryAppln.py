###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Discovery application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods. etc.
#
# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. One
# of the parameters should be the total number of Discoverys and subscribers
# in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See Discovery code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save
# the registrations.
# (5) When all the Discoverys and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then
# it will be false.

import argparse  # for argument parsing
import logging  # for logging. Use it in place of print statements.
from enum import Enum  # for an enumeration we are using to describe what state we are i
import configparser  # for parsing the config file
# Now import our CS6381 Middleware
from CS6381_MW.DiscoveryMW import DiscoveryMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2


class DiscoveryAppln():
    """Application logic for the Discovery service."""
    class State (Enum):
        """Enumeration for the state of the Discovery service."""
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        DISSEMINATE = 4,
        COMPLETED = 5

    # initialize the logger
    def __init__(self, logger):
        self.logger = logger
        self.subscribers = {}
        self.publishers = {}
        self.ready = False
        self.state = self.State.INITIALIZE
        self.number_of_pubsub = 0
        self.lookup = None
        self.dissemination = None
        self.mw_obj = None
        self.curr_registered = 0
        self.broker = []
    # configure the application
    def configure(self, args):
        """Configure the application."""
        self.logger.debug(f'DiscoveryAppln: configure: args: {args}')
        self.state = self.State.CONFIGURE
        self.number_of_pubsub = args.numpubsub
        self.logger.debug("DiscoveryAppln::configure - parsing config.ini")
        config = configparser.ConfigParser()
        config.read(args.config)
        self.lookup = config["Discovery"]["Strategy"]
        self.dissemination = config["Dissemination"]["Strategy"]
        # Now setup up our underlying middleware object to which we delegate
        # everything
        self.logger.debug(
            "DiscoveryAppln::configure - initialize the middleware object")
        self.mw_obj = DiscoveryMW(self.logger)
        # pass remainder of the args to the m/w object
        self.mw_obj.configure(args)

        self.logger.info("DiscoveryAppln::configure - configuration complete")

    def driver(self):
        """Driver for the Discovery service."""
        self.logger.debug("DiscoveryAppln: driver")
        self.state = self.State.REGISTER
        self.mw_obj.set_upcall_handle(self)

        self.mw_obj.recv_messages()

    def handle_messages(self, msg):
        """Handle incoming messages."""
        self.logger.info("DiscoveryAppln: handle_messages")
        self.logger.debug(
            f'DiscoveryAppln: handle_messages - msg: {msg}')
        # Now we need to parse the message and determine what method was invoked
        # and then call the appropriate method to handle it
        print("reciveing messages", msg.msg_type)
        if msg.msg_type == 1:
            self.logger.debug("DiscoveryAppln: handle_messages - register")
            return self.register(msg)
        elif msg.msg_type == 2:
            self.logger.debug("DiscoveryAppln: handle_messages - is_ready")
            return self.is_ready(msg)
        elif msg.msg_type == 3:
            self.logger.debug("DiscoveryAppln: handle_messages - lookup")
            print('returning lookup response')
            return self.lookup_response(msg)
        else:
            self.logger.error(
                "DiscoveryAppln: handle_messages - unknown method")
        return None

    def register(self, msg):
        """Handle register request."""
        self.logger.debug("DiscoveryAppln: register")
        self.logger.debug(f'DiscoveryAppln: register - msg: {msg}')
        # Now we need to parse the message and determine what method was invoked
        # and then call the appropriate method to handle it
        if msg.msg_type == 1:
            self.logger.debug("DiscoveryAppln: register - register")
            # return a response
            sender_id = msg.register_req.info.id

            # send respoinse
            register_resp = discovery_pb2.RegisterResp()  # pylint: disable=no-member
            register_resp.status = 1
            register_resp.reason = "Success"
            disc_resp = discovery_pb2.DiscoveryResp()  # pylint: disable=no-member
            disc_resp.register_resp.CopyFrom(register_resp)
            disc_resp.msg_type = discovery_pb2.TYPE_REGISTER  # pylint: disable=no-member
            self.curr_registered +=1
            if msg.register_req.role == 1:
                print('recived request to register publisher')
                print(msg.register_req.info)
                self.publishers[sender_id] = {"address": msg.register_req.info.addr,
                 "port": msg.register_req.info.port, "topics": msg.register_req.topiclist}
            elif msg.register_req.role == 3:
                print('recived request to register broker')
                self.broker.append(msg.register_req.info.addr + ":" + str(msg.register_req.info.port))
            else:
                print('recived request to register subscriber')
            return disc_resp
        else:
            self.logger.error(
                "DiscoveryAppln: register - unknown method")
        return None

    def is_ready(self, msg):
        """Handle is_ready request."""
        self.logger.debug("DiscoveryAppln: is_ready")
        self.logger.debug(f'DiscoveryAppln: is_ready - msg: {msg}')
        # Now we need to parse the message and determine what method was invoked
        # and then call the appropriate method to handle it
        if msg.msg_type == 2:
            self.logger.debug("DiscoveryAppln: is_ready - is_ready")
            # return a response

            is_ready_resp = discovery_pb2.IsReadyResp() # pylint: disable=no-member
            print("SETTING THE STATUS", self.curr_registered, self.number_of_pubsub)

            is_ready_resp.status = True
           
            disc_resp = discovery_pb2.DiscoveryResp()  # pylint: disable=no-member
            disc_resp.isready_resp.CopyFrom(is_ready_resp)
            disc_resp.msg_type = discovery_pb2.TYPE_ISREADY  # pylint: disable=no-member
            return disc_resp
        return None

    def lookup_response(self, msg):
        """Handle lookup request."""
        print('lookup response')
        self.logger.debug("DiscoveryAppln: lookup")
        self.logger.debug(f'DiscoveryAppln: lookup - msg: {msg}')
        # Now we need to parse the message and determine what method was invoked
        # and then call the appropriate method to handle it
        if msg.msg_type == 3:
            self.logger.debug("DiscoveryAppln: lookup - lookup")
            topics = msg.lookup_req.topiclist
            print('recived lookup request for topics', topics)
            lookup_resp = discovery_pb2.LookupPubByTopicResp()  # pylint: disable=no-member
            pubs_array = []
            addr, ports = [], []
            flag = True
            if len(topics)==1 and topics[0]=="broker_overrides":
                print("broker_overrides")
                if not self.broker:
                    flag = False
                else:
                    pubs_array.append("broker")
                    for broker in self.broker:
                        addr.append(broker.split(":")[0])
                        ports.append(broker.split(":")[1])
            else:
                print(self.publishers)
                print(type(self.publishers))
                for key, val in self.publishers.items():
                    for topic in topics:
                        if topic in val['topics']:
                            print('found publisher for topic', topic)
                            if key not in pubs_array:
                                pubs_array.append(key)

                                addr.append(val['address'])  #[ip1, ip2], [port1, port2]
                                ports.append(str(val['port']))

            pubs_array = list(set(pubs_array))
            print("PRINTING PUBS ARRAY")
            print(self.curr_registered)
            lookup_resp.pubname[:] = pubs_array

            lookup_resp.addr[:] = addr
            lookup_resp.port[:] = ports
            print(self.curr_registered)
   
            lookup_resp.status = True
            print(lookup_resp.status)
            disc_resp = discovery_pb2.DiscoveryResp()  # pylint: disable=no-member
            disc_resp.lookup_resp.CopyFrom(lookup_resp)
            disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC  # pylint: disable=no-member
            return disc_resp
    
    def get_publishers(self):
        '''returns the publishers'''
        return self.publishers

    def update_publisher_data(self, data):
        '''updates the publishers'''
        self.publishers = data
        print(self.publishers)

    def update_broker(self, broker):
        '''updates the broker'''
        self.broker.append(broker)


def main():

    """Main driver for the Discovery service."""
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info(
            "Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("DiscoveryAppln")

        # first parse the arguments
        logger.debug("Main: parse command line arguments")
        args = parse_cmd_line_args()

        # reset the log level to as specified
        logger.debug('Main: resetting log level to %s', args.loglevel)
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is %s",
                     logger.getEffectiveLevel())

        # Obtain a Discovery application
        logger.debug("Main: obtain the Discovery appln object")
        pub_app = DiscoveryAppln(logger)

        # configure the object
        logger.debug("Main: configure the Discovery appln object")
        pub_app.configure(args)

        # now invoke the driver program
        logger.debug("Main: invoke the Discovery appln driver")
        pub_app.driver()

    except Exception as excpt: #pylint: disable=broad-except
        logger.error("Exception caught in main - %s", excpt)
        return


def parse_cmd_line_args():
    """Parse command line arguments."""
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Discovery Application")

    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)

    parser.add_argument("-c", "--config", default="config.ini",
                        help="configuration file (default: config.ini)")

    parser.add_argument("-t", "--numpubsub", type=int, default=2,
                        help="number of subscribers and Discoverys (default: 1000)")

    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[
                        logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR,
                        logging.CRITICAL],
                        help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

    parser.add_argument("-a", "--addr", default="localhost",
                        help="IP addr of this Discovery to advertise (default: localhost)")

    parser.add_argument("-p", "--port", type=int, default=5555,
                        help="Port number on which our underlying" +
                        "Discovery ZMQ service runs, default=5555")
    parser.add_argument("-b", "--broker", default="localhost:8087",
                        help="IP Addr:Port combo for the broker service, default localhost:8087")
    parser.add_argument("-n", "--name", default="discovery1",
                        help="IP Addr:Port combo for the broker service, default localhost:8087")                        
    return parser.parse_args()


if __name__ == "__main__":

    # set underlying default logging capabilities
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    main()
