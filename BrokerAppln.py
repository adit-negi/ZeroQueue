###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Broker application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers. 


import argparse  # for argument parsing
import logging  # for logging. Use it in place of print statements.
from enum import Enum  # for an enumeration we are using to describe what state we are i
import time  # for parsing the config file
# Now import our CS6381 Middleware
from CS6381_MW.BrokerMW import BrokerMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2
from topic_selector import TopicSelector


class  BrokerAppln():
    """Application logic for the Broker service."""
    class State (Enum):
        """Enumeration for the state of the Broker service."""
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        LOOKUP = 3,
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
        self.ports = None
        self.addrs = None
        self.dissemination = None
        self.mw_obj = None
        self.curr_registered = 0
        self.topiclist = []
    # configure the application
    def configure(self, args):
        """Configure the application."""
        self.logger.debug(f'BrokerAppln: configure: args: {args}')
        self.state = self.State.CONFIGURE

        self.logger.debug("BrokerAppln::configure - parsing config.ini")


        
        self.topiclist = ["weather", "humidity", "airquality", "light",
                            "pressure", "temperature", "sound", "altitude",
                            "location"]
        print('here22')
        # Now setup up our underlying middleware object to which we delegate
        # everything
        self.logger.debug(
            "BrokerAppln::configure - initialize the middleware object")
        self.mw_obj = BrokerMW(self.logger)
        # pass remainder of the args to the m/w object
        self.mw_obj.configure(args)

        self.logger.info("BrokerAppln::configure - configuration complete")

    def driver(self):
        """Driver for the Broker service."""
        self.logger.debug("BrokerAppln: driver")
        self.state = self.State.REGISTER
        self.mw_obj.set_upcall_handle(self)

        self.mw_obj.event_loop(timeout=0)  # start the event loop

    def invoke_operation(self):
        ''' Invoke operating depending on state  '''

        try:
            self.logger.info("SubscriberAppln::invoke_operation")

            # check what state are we in. If we are in REGISTER state,
            # we send register request to discovery service. If we are in
            # ISREADY state, then we keep checking with the discovery
            # service.
            if self.state == self.State.REGISTER:
                # Now keep checking with the discovery service if we are ready to go
                #
                # Note that in the previous version of the code, we had a loop. But now instead
                # of an explicit loop we are going to go back and forth between the event loop
                # and the upcall until we receive the go ahead from the discovery service.

                self.logger.debug(
                    "SubscriberAppln::invoke_operation - check if are ready to go")
                self.mw_obj.register()  # send the lookup? request

                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a isready request, the very next thing we expect is
                # to receive a response from remote entity. So we need to set the timeout
                # for the next iteration of the event loop to a large num and so return a None.
                return None
            elif self.state == self.State.LOOKUP:
                # Now keep checking with the discovery service if we are ready to go
                #
                # Note that in the previous version of the code, we had a loop. But now instead
                # of an explicit loop we are going to go back and forth between the event loop
                # and the upcall until we receive the go ahead from the discovery service.

                self.logger.debug(
                    "SubscriberAppln::invoke_operation - check if are ready to go")
                self.mw_obj.lookup(self.topiclist)  # send the lookup? request

                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a isready request, the very next thing we expect is
                # to receive a response from remote entity. So we need to set the timeout
                # for the next iteration of the event loop to a large num and so return a None.
                return None

            elif self.state == self.State.DISSEMINATE:
                # send a register msg to discovery service
                self.logger.debug(
                    "SubscriberAppln::invoke_operation - register with the discovery service")
                self.mw_obj.dissimenate(self.ports, self.addrs, self.topiclist)

                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a register request, the very next thing we expect is
                # to receive a response from remote entity. So we need to set the timeout
                # for the next iteration of the event loop to a large num and so return a None.
                return None


            else:
                raise ValueError("Undefined state of the appln object")

            self.logger.info("SubscriberAppln::invoke_operation completed")
        except Exception as e: # pylint: disable=invalid-name
            raise e

    def lookup_response(self, lookup_resp):
        """Handle register request."""
        self.logger.debug("BrokerAppln: register")
        self.logger.debug(f'BrokerAppln: register - msg: {lookup_resp}')
        # Now we need to parse the message and determine what method was invoked
        # and then call the appropriate method to handle it

        try:
            self.logger.info("SubscriberAppln::lookup_response")

            # Notice how we get that loop effect with the sleep (10)
            # by an interaction between the event loop and these
            # upcall methods.
            print(lookup_resp.port)
            if not lookup_resp.status:
                # discovery service is not ready yet
                self.logger.debug(
                    "SubscriberAppln::driver - Not ready yet; check again")
                # sleep between calls so that we don't make excessive calls
                time.sleep(10)

            else:
                # we got the go ahead
                # set the state to disseminate
                self.logger.info("SubscriberAppln::GOT THE LOOKUP RESPONSE LETS CONNECT")
                if len(lookup_resp.port[:]) == 0:
                    self.logger.info("SubscriberAppln::NO PUBLISHERS")

                else:
                    self.ports = lookup_resp.port[:]
                    self.addrs = lookup_resp.addr[:]
                    self.state = self.State.DISSEMINATE
                    print("CHANGING STATE TO LISTEN")


            # return timeout of 0 so event loop calls us back in the invoke_operation
            # method, where we take action based on what state we are in.
            return 0

        except Exception as e: #pylint: disable=invalid-name
            raise e
    

    def register_response(self, reg_resp):
        ''' handle register response '''

        try:
            self.logger.info("PublisherAppln::register_response")
            if reg_resp.status == discovery_pb2.STATUS_SUCCESS: # pylint: disable=no-member
                self.logger.debug(
                    "PublisherAppln::register_response - registration is a success")

                # set our next state to isready so that we can then
                # send the isready message right away
                self.state = self.State.LOOKUP

                # return a timeout of zero so that the event loop in its
                # next iteration will immediately make
                # an upcall to us
                return 0

            else:
                self.logger.debug(
                    "PublisherAppln::register_response - registration is a failure with reason %s",
                    reg_resp.reason)
                raise ValueError("Publisher needs to have unique id")

        except Exception as e: # pylint: disable=invalid-name
            raise e


def main():

    """Main driver for the Broker service."""
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info(
            "Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("BrokerAppln")

        # first parse the arguments
        logger.debug("Main: parse command line arguments")
        args = parse_cmd_line_args()

        # reset the log level to as specified
        logger.debug('Main: resetting log level to %s', args.loglevel)
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is %s",
                     logger.getEffectiveLevel())

        # Obtain a Broker application
        logger.debug("Main: obtain the Broker appln object")
        pub_app = BrokerAppln(logger)

        # configure the object
        logger.debug("Main: configure the Broker appln object")
        pub_app.configure(args)

        # now invoke the driver program
        logger.debug("Main: invoke the Broker appln driver")
        pub_app.driver()

    except Exception as excpt: #pylint: disable=broad-except
        logger.error("Exception caught in main - %s", excpt)
        return


def parse_cmd_line_args():
    """Parse command line arguments."""
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Broker Application")

    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)

    parser.add_argument("-c", "--config", default="config.ini",
                        help="configuration file (default: config.ini)")


    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[
                        logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR,
                        logging.CRITICAL],
                        help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

    parser.add_argument("-a", "--addr", default="localhost",
                        help="IP addr of this Broker to advertise (default: localhost)")
    parser.add_argument("-n", "--name", default="bro1",
                        help="name of broker")
    parser.add_argument("-p", "--port", type=int, default=8087,
                        help="Port number on which our underlying" +
                        "Broker ZMQ service runs, default=8087")
    parser.add_argument("-d", "--discovery", default="localhost:5555",
                        help="IP Addr:Port combo for the discovery service, default localhost:5555")
    return parser.parse_args()


if __name__ == "__main__":

    # set underlying default logging capabilities
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    main()
