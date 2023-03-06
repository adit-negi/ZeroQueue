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

import argparse
import json  # for argument parsing
import logging  # for logging. Use it in place of print statements.
from enum import Enum  # for an enumeration we are using to describe what state we are i
import configparser
import time  # for parsing the config file
# Now import our CS6381 Middleware
from CS6381_MW.DiscoveryMW import DiscoveryMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2
from CS6381_MW.Common import Chord


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
        self.broker = None
        self.pubs_by_topic = {}
        self.configured_pubs = 5
        
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
        f = open('finger_table.json', 'r')
        dht = open('hash_generator/dht.json', 'r')
        dht_data = json.load(dht)
        data = json.load(f)
        # pass remainder of the args to the m/w object
        self.mw_obj.configure(args, data, dht_data)

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
        # logic to check if it is the correct node to handle the message
        # else forward the message to the correct node

        if msg.msg_type == 1 or msg.msg_type == 5:
            self.logger.debug("DiscoveryAppln: handle_messages - register")
            return self.register(msg)
        elif msg.msg_type == 2:
            self.logger.debug("DiscoveryAppln: handle_messages - is_ready")
            return self.is_ready(msg)
        elif msg.msg_type == 3:
            self.logger.debug("DiscoveryAppln: handle_messages - lookup")
            print('returning lookup response')
            return self.lookup_response(msg)
        elif msg.msg_type == 6:
            self.logger.info("DiscoveryAppln: handle_messages - lookup")
            return self.discovery_lookup_response(msg)
        else:
            self.logger.info(
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
            if msg.register_req.role == 1:
                print('recived request to register publisher')
                print(msg.register_req.info)
                self.publishers[sender_id] = {"address": msg.register_req.info.addr,
                 "port": msg.register_req.info.port, "topics": msg.register_req.topiclist}
            elif msg.register_req.role == 3:
                print('recived request to register broker')
                self.broker = msg.register_req.info.addr + ":" + str(msg.register_req.info.port)
                json_object = json.dumps({"broker":self.broker}, indent=4)
                # Writing to sample.json
                with open("broker.json", "w") as outfile:
                    outfile.write(json_object)
            else:
                print('recived request to register subscriber')
            return disc_resp
        elif msg.msg_type == 5:
            register_resp = discovery_pb2.RegisterResp()  # pylint: disable=no-member
            register_resp.status = 1
            register_resp.reason = "Success"
            disc_resp = discovery_pb2.DiscoveryResp()  # pylint: disable=no-member
            disc_resp.register_resp.CopyFrom(register_resp)
            disc_resp.msg_type = discovery_pb2.TYPE_REGISTER  # pylint: disable=no-member
            self.logger.info('sending back response')
            return disc_resp
        else:
            self.logger.info(
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
            #check if is ready by calling the middleware
            ready_flag = self.mw_obj.system_ready()
            print(ready_flag)
            is_ready_resp = discovery_pb2.IsReadyResp() # pylint: disable=no-member


            is_ready_resp.status = ready_flag


            if self.dissemination !="Direct" and self.broker is None:
                is_ready_resp.status = False
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
            if self.mw_obj.system_ready():
                self.ready = True
            
            print(self.ready)
            print(self.curr_registered)


            lookup_resp = discovery_pb2.LookupPubByTopicResp()  # pylint: disable=no-member
            pubs_array = []
            addr, ports = [], []
            flag = True
            if len(topics)==1 and topics[0]=="broker_overrides":
                print("broker_overrides")
                # Opening JSON file
                with open('broker.json', 'r') as openfile:
                
                    # Reading from json file
                    json_object = json.load(openfile)
                    if "broker" in json_object:
                        self.broker = json_object["broker"]
                if not self.broker:
                    flag = False
                else:
                    pubs_array.append("broker")
                    addr.append(self.broker.split(":")[0])
                    ports.append(self.broker.split(":")[1])
            else:
                # logic to find publishers by topic
                for topic in topics:
                    topic_hash = self.mw_obj.hash_func(topic)
                    # for each topic hash find the finger node and forward the request
                    self.logger.info(self.pubs_by_topic)
                    if topic not in self.pubs_by_topic:
                        self.mw_obj.curr_node_handle_lookup(topic_hash, topic)
                    
                    print(topic)
                    print("CHECKING AGAIN")
                    #check again
                    if topic in self.pubs_by_topic:

                        for pub in self.pubs_by_topic[topic]:
                            pubs_array.append(pub["name"])
                            addr.append(pub["ip"])
                            ports.append(pub["port"])
                  

            pubs_array = list(set(pubs_array))

            lookup_resp.pubname[:] = pubs_array

            lookup_resp.addr[:] = addr
            lookup_resp.port[:] = ports
            print(self.curr_registered)
            if not flag:
                lookup_resp.status = 0
            else:
                lookup_resp.status = 1
            if len(pubs_array)<1:
                lookup_resp.status = 0
            print(lookup_resp.status)
            disc_resp = discovery_pb2.DiscoveryResp()  # pylint: disable=no-member
            disc_resp.lookup_resp.CopyFrom(lookup_resp)
            disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC  # pylint: disable=no-member
            return disc_resp

    def discovery_lookup_response(self, msg):
        """Handle lookup request."""
        print(msg)
        topic = msg.disc_reg_req.topic
        pubs_array = []
        addr, ports = [], []
        print(msg)
        print(topic)
        print("HANDLING DISCOVERY LOOKUP")
        if topic in self.pubs_by_topic:
            for pub in self.pubs_by_topic[topic]:
                pubs_array.append(pub["name"])
                addr.append(pub["ip"])
                ports.append(pub["port"])
            lookup_resp = discovery_pb2.LookupPubByTopicResp()  # pylint: disable=no-member
            lookup_resp.pubname[:] = pubs_array

            lookup_resp.addr[:] = addr
            lookup_resp.port[:] = ports
            lookup_resp.status = 1
            disc_resp = discovery_pb2.DiscoveryResp()  # pylint: disable=no-member
            disc_resp.lookup_resp.CopyFrom(lookup_resp)
            disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC  # pylint: disable=no-member
            return disc_resp
        else:
            print("no publishers yet", topic)
            print(self.pubs_by_topic)
            lookup_resp = discovery_pb2.LookupPubByTopicResp()  # pylint: disable=no-member
            lookup_resp.pubname[:] = pubs_array

            lookup_resp.addr[:] = addr
            lookup_resp.port[:] = ports
            lookup_resp.status = 1
            disc_resp = discovery_pb2.DiscoveryResp()  # pylint: disable=no-member
            disc_resp.lookup_resp.CopyFrom(lookup_resp)
            disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC  # pylint: disable=no-member
            return disc_resp

    def handle_register_message(self, topic, topic_hash, ip, port, role):

        """Handle register message from another discovery node."""
        if role ==2 or role=="2":
            return
        pubname = ip + ":" + str(port)
        if pubname in self.publishers:
            if topic not in self.publishers[pubname]["topics"]:
                self.publishers[pubname]["topics"].append(topic)
        else:
            self.publishers[pubname] = {"address": ip, "port": port, "topics": [topic]}

        if topic not in self.pubs_by_topic:
            self.pubs_by_topic[topic] = [{'name':pubname, 'ip':ip, 'port':str(port)}]
        else:
            if pubname not in self.pubs_by_topic[topic]:
                self.pubs_by_topic[topic].append({'name':pubname, 'ip':ip, 'port':str(port)})

    def update_register_counter(self):
        """Update the register counter."""
        self.curr_registered+=1
    def is_ready_flag(self):
        """Return the is_ready flag."""
        self.logger.info('getting is ready request internally')
        self.logger.info('curr_registered', self.curr_registered)
        return self.curr_registered>=self.number_of_pubsub

    def handle_lookup_message(self, hash_topic, topic):
        """Handle lookup message from another discovery node."""
        if topic in self.pubs_by_topic:
            print("YES WE HAVE THE TOPIC")
        else:
            print("we don't have the topic")

    def handle_lookup_resposne(self, msg, topic):
        '''handle lookup response'''
        pubs = msg.lookup_resp.pubname
        ports = msg.lookup_resp.port
        addr = msg.lookup_resp.addr
        if topic not in self.pubs_by_topic:
            self.pubs_by_topic[topic] =[]
        for i, _ in enumerate(pubs):
            f = False
            for pub in self.pubs_by_topic[topic]:
                if pub['name'] == pubs[i]:
                    f = True
                    break
            if f:
                continue
            self.pubs_by_topic[topic].append({'name':pubs[i], 'ip':addr[i], 'port':str(ports[i])})

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

    parser.add_argument("-t", "--numpubsub", type=int, default=5,
                        help="number of subscribers and Discoverys (default: 1000)")

    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[
                        logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR,
                        logging.CRITICAL],
                        help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

    parser.add_argument("-a", "--addr", default="localhost",
                        help="IP addr of this Discovery to advertise (default: localhost)")

    parser.add_argument("-n", "--name", default="disc3",
                        help="name of the dht node (default: disc1)")

    parser.add_argument("-p", "--port", type=int, default=5557,
                        help="Port number on which our underlying" +
                        "Discovery ZMQ service runs, default=5555")
    parser.add_argument("-b", "--broker", default="localhost:8087",
                        help="IP Addr:Port combo for the broker service, default localhost:8087")

    return parser.parse_args()


if __name__ == "__main__":

    # set underlying default logging capabilities
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    main()
