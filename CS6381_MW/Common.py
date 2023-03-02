###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the publisher middleware code
#
# Created: Spring 2023
#
###############################################

# This file contains any declarations that are common to all middleware entities

# For now we do not have anything here but you can add enumerated constants for
# the role we are playing and any other common things that we need across
# all our middleware objects. Make sure then to import this file in those files once
# some content is added here that is needed by others. 


class Chord:
    '''Chord class to implement the chord algorithm'''
    def __init__(self):
        self.predecessor = None
        self.successor = None
        self.finger_table = None
        self.data = None


    def chord_algo(self, hash_value, finger_table, curr_hash):
        '''This function implements the chord algorithm'''
        # find the node which is smallr than the hash value
        print(finger_table)
        print(hash_value)
        curr_min =10e19
        value_to_return = None
        #first check if successor can handle the request
        if finger_table["1"]['hash_value'] >= hash_value:
            return finger_table['1']['id']

        #successor is smaller than the hash value
        if finger_table["1"]['hash_value'] < curr_hash and hash_value > curr_hash:
            return finger_table['1']['id']

        #if not, find the node which is smaller than the hash value
        print('here')
        for finger in finger_table:
            if finger == 'predecessor':
                continue
            finger = finger_table[finger]
            if finger['hash_value'] <= hash_value:
                if curr_min > hash_value - finger['hash_value']:

                    curr_min = min(curr_min, hash_value - finger['hash_value'])
                    value_to_return = finger['id']

        print('never reacher here')
        return value_to_return
