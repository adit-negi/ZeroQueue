###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise to the student. Design the logic in a manner similar
# to the SubscriberAppln. As in the Subscriber application, the subscriber application
# will maintain a handle to the underlying subscriber middleware object.
#
# The key steps for the subscriber application are
# (1) parse command line and configure application level parameters
# (2) obtain the subscriber middleware object and configure it.
# (3) As in the Subscriber, register ourselves with the discovery service
# (4) since we are a subscriber, we need to ask the discovery service to
# let us know of each Subscriber that publishes the topic of interest to us. Then
# our middleware object will connect its SUB socket to all these Subscribers
# for the Direct strategy else connect just to the broker.
# (5) Subscriber will always be in an event loop waiting for some matching
# publication to show up. We also compute the latency for dissemination and
# store all these time series data in some database for later analytics.

# import the needed packages
import time   # for sleep
import argparse  # for argument parsing
import configparser  # for configuration parsing
import logging  # for logging. Use it in place of print statements.
from enum import Enum  # for an enumeration we are using to describe what state we are in
# Import our topic selector. Feel free to use alternate way to
# get your topics of interest
from topic_selector import TopicSelector

# Now import our CS6381 Middleware
from CS6381_MW.SubscriberMW import SubscriberMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.


##################################
#       SubscriberAppln class
##################################


class SubscriberAppln ():
    """Application logic for the Subscriber service."""
    # these are the states through which our Subscriber appln object goes thru.
    # We maintain the state so we know where we are in the lifecycle and then
    # take decisions accordingly
    class State (Enum):
        '''Enumeration of the states that we are in'''
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        LOOKUP = 3,
        LISTEN = 4,
        COMPLETED = 5

    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.state = self.State.INITIALIZE  # state that are we in
        self.name = None  # our name (some unique name)
        self.topiclist = None  # the different topics that we are interested in
        self.iters = None   # number of iterations of publication
        self.lookup = None  # one of the diff ways we do lookup
        self.mw_obj = None  # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements
        self.dissemination = None  # dissemination strategy
        self.num_topics = None  # number of topics we are interested in
        self.publisher_data = {}  # dictionary of publisher data
        self.ports = []
        self.addrs = []
        self.sleep = 10
    ########################################
    # configure/initialize
    ########################################
    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("SubscriberAppln::configure")

            # set our current state to CONFIGURE state
            self.state = self.State.CONFIGURE

            # initialize our variables
            self.name = args.name  # our name
            self.iters = args.iters  # num of iterations

            self.num_topics = args.num_topics  # num of topics we are interested in

            # Now, get the configuration object
            self.logger.debug("SubscriberAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]
            # Now get our topic list of interest
            self.logger.debug(
                "SubscriberAppln::configure - selecting our topic list")
            topic_selector = TopicSelector()
            # let topic selector give us the desired num of topics
            self.topiclist = topic_selector.interest(self.num_topics)


            # Now setup up our underlying middleware object to which we delegate
            # everything
            self.logger.debug(
                "SubscriberAppln::configure - initialize the middleware object")
            self.mw_obj = SubscriberMW(self.logger)
            # pass remainder of the args to the m/w object
            self.mw_obj.configure(args)

            self.logger.info(
                "SubscriberAppln::configure - configuration complete")

        except Exception as excpt:
            raise excpt

    ########################################
    # driver program
    ########################################
    def driver(self):
        ''' Driver program '''

        try:
            self.logger.info("SubscriberAppln::driver")

            # dump our contents (debugging purposes)
            self.dump()

            # First ask our middleware to keep a handle to us to make upcalls.
            # This is related to upcalls. By passing a pointer to ourselves, the
            # middleware will keep track of it and any time something must
            # be handled by the application level, invoke an upcall.
            self.logger.debug("SubscriberAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)

            # the next thing we should be doing is to register with the discovery
            # service. But because we are simply delegating everything to an event loop
            # that will call us back, we will need to know when we get called back as to
            # what should be our next set of actions.  Hence, as a hint, we set our state
            # accordingly so that when we are out of the event loop, we know what
            # operation is to be performed.  In this case we should be registering with
            # the discovery service. So this is our next state.
            self.state = self.State.REGISTER

            # Now simply let the underlying middleware object enter the event loop
            # to handle events. However, a trick we play here is that we provide a timeout
            # of zero so that control is immediately sent back to us where we can then
            # register with the discovery service and then pass control back to the event loop
            #
            # As a rule, whenever we expect a reply from remote entity, we set timeout to
            # None or some large value, but if we want to send a request ourselves right away,
            # we set timeout is zero.
            #
            self.mw_obj.event_loop(timeout=0)  # start the event loop

            self.logger.info("SubscriberAppln::driver completed")

        except Exception as e: # pylint: disable=invalid-name
            raise e

    ########################################
    # generic invoke method called as part of upcall
    #
    # This method will get invoked as part of the upcall made
    # by the middleware's event loop after it sees a timeout has
    # occurred.
    ########################################
    def invoke_operation(self):
        ''' Invoke operating depending on state  '''

        try:
            self.logger.info("SubscriberAppln::invoke_operation")

            # check what state are we in. If we are in REGISTER state,
            # we send register request to discovery service. If we are in
            # ISREADY state, then we keep checking with the discovery
            # service.
            if self.state == self.State.REGISTER:
                # send a register msg to discovery service
                self.logger.debug(
                    "SubscriberAppln::invoke_operation - register with the discovery service")
                self.mw_obj.register(self.name, self.topiclist)

                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a register request, the very next thing we expect is
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
                if self.dissemination =='Direct':
                    self.mw_obj.lookup(self.topiclist)  # send the lookup? request
                else:
                    # send the lookup? request with broker overrides
                    self.mw_obj.lookup(['broker_overrides'])

                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a isready request, the very next thing we expect is
                # to receive a response from remote entity. So we need to set the timeout
                # for the next iteration of the event loop to a large num and so return a None.
                return None

            elif self.state == self.State.LISTEN:
                self.logger.debug(
                    "SubscriberAppln::invoke_operation - connect to publishers")
                self.logger.info("SubscriberAppln::PUBLISHERS")
                self.mw_obj.connect(self.ports,
                    self.addrs, self.topiclist)
                # we are done. So we move to the completed state

                # return a timeout of zero so that the event loop
                # sends control back to us right away.
                return None

            elif self.state == self.State.COMPLETED:

                # we are done. Time to break the event loop.
                # So we created this special method on the
                # middleware object to kill its event loolookupp
                self.mw_obj.disable_event_loop()
                return None

            else:
                raise ValueError("Undefined state of the appln object")

            self.logger.info("SubscriberAppln::invoke_operation completed")
        except Exception as e: # pylint: disable=invalid-name
            raise e

    ########################################
    # handle register response method called as part of upcall
    #
    # As mentioned in class, the middleware object can do the reading
    # from socket and deserialization. But it does not know the semantics
    # of the message and what should be done. So it becomes the job
    # of the application. Hence this upcall is made to us.
    ########################################
    def register_response(self, reg_resp):
        ''' handle register response '''

        try:
            self.logger.info("SubscriberAppln::register_response")
            if reg_resp.status == discovery_pb2.STATUS_SUCCESS: # pylint: disable=no-member
                self.logger.debug(
                    "SubscriberAppln::register_response - registration is a success")

                # set our next state to isready so that we can then
                # send the isready message right away
                self.state = self.State.LOOKUP

                # return a timeout of zero so that the event loop in its
                # next iteration will immediately make
                # an upcall to us
                return 0

            else:
                self.logger.debug(
                    "SubscriberAppln::register_response - registration is a failure with reason %s",
                    reg_resp.reason)
                raise ValueError("Subscriber needs to have unique id")

        except Exception as e: # pylint: disable=invalid-name
            raise e

    ########################################
    # handle isready response method called as part of upcall
    #
    # Also a part of upcall handled by application logic
    ########################################
    def lookup_response(self, lookup_resp):
        ''' handle isready response '''

        try:
            self.logger.info("SubscriberAppln::lookup_response")
            self.logger.info(lookup_resp)
            # Notice how we get that loop effect with the sleep (10)
            # by an interaction between the event loop and these
            # upcall methods.
            self.logger.info(lookup_resp.port)
            if not lookup_resp.status:
                # discovery service is not ready yet
                self.logger.info(
                    "SubscriberAppln::driver - Not ready yet; check again")
                # sleep between calls so that we don't make excessive calls

                time.sleep(self.sleep)
                # increase the sleep time so that we don't make excessive calls
                self.sleep += 5

            else:
                # we got the go ahead
                # set the state to disseminate
                self.logger.info("SubscriberAppln::GOT THE LOOKUP RESPONSE LETS CONNECT")
                if len(lookup_resp.port[:]) == 0:
                    self.logger.info("SubscriberAppln::NO PUBLISHERS")

                else:
                    self.ports = lookup_resp.port[:]
                    self.addrs = lookup_resp.addr[:]
                    self.state = self.State.LISTEN
                    print("CHANGING STATE TO LISTEN")


            # return timeout of 0 so event loop calls us back in the invoke_operation
            # method, where we take action based on what state we are in.
            return 0

        except Exception as e: #pylint: disable=invalid-name
            raise e

    ########################################
    # dump the contents of the object
    ########################################
    def dump(self):
        ''' Pretty print '''

        try:
            self.logger.info("**********************************")
            self.logger.info("SubscriberAppln::dump")
            self.logger.info("------------------------------")
            self.logger.info("     Name: %s", self.name)
            self.logger.info("     Lookup: %s", self.lookup)
            self.logger.info(
                "     Dissemination: %s", self.dissemination)
            self.logger.info("     Num Topics: %s", self.num_topics)
            self.logger.info("     TopicList: %s", self.topiclist)
            self.logger.info("     Iterations: %s", self.iters)
            self.logger.info("**********************************")

        except Exception as e: # pylint: disable=invalid-name
            raise e

###################################
#
# Parse command line arguments
#
###################################


def parse_cmd_line_args():
    '''instantiate a ArgumentParser object'''
    parser = argparse.ArgumentParser(description="Subscriber Application")

    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)

    parser.add_argument("-n", "--name", default="pub",
                        help="Some name assigned to us. Keep it unique per Subscriber")

    parser.add_argument("-a", "--addr", default="localhost",
                        help="IP addr of this Subscriber to advertise (default: localhost)")

    parser.add_argument("-p", "--port", type=int, default=6577,
                        help="Port number on which our underlying" +
                        "Subscriber ZMQ service runs, default=6577")

    parser.add_argument("-d", "--discovery", default="localhost:5555",
                        help="IP Addr:Port combo for the discovery service, default localhost:5555")

    parser.add_argument("-T", "--num_topics", type=int, choices=range(1, 10), default=1,
                        help="Number of topics to publish, currently restricted to max of 9")

    parser.add_argument("-c", "--config", default="config.ini",
                        help="configuration file (default: config.ini)")

    parser.add_argument("-f", "--frequency", type=int, default=1,
                        help="Rate at which topics disseminated:" +
                        "default once a second - use integers")

    parser.add_argument("-i", "--iters", type=int, default=1000,
                        help="number of publication iterations (default: 1000)")

    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[
                        logging.DEBUG, logging.INFO,
                        logging.WARNING, logging.ERROR, logging.CRITICAL],
                        help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

    return parser.parse_args()


###################################
#
# Main program
#
###################################
def main():
    ''' main program '''
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info(
            "Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("SubscriberAppln")

        # first parse the arguments
        logger.debug("Main: parse command line arguments")
        args = parse_cmd_line_args()

        # reset the log level to as specified
        logger.debug("Main: resetting log level to %s", args.loglevel)
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is %s",
            logger.getEffectiveLevel())

        # Obtain a Subscriber application
        logger.debug("Main: obtain the Subscriber appln object")
        pub_app = SubscriberAppln(logger)

        # configure the object
        logger.debug("Main: configure the Subscriber appln object")
        pub_app.configure(args)

        # now invoke the driver program
        logger.debug("Main: invoke the Subscriber appln driver")
        pub_app.driver()
    # pylint: disable=invalid-name
    except Exception as e: # pylint: disable=broad-except
        logger.error("Exception caught in main - %s", e)
        return


###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":

    # set underlying default logging capabilities
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    main()
