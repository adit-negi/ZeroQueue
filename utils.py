'''This file is for helper functions'''
import json
import random


def find_node_hash(hash_val):
    '''This function constructs the finger table for each node in the DHT'''
    # open dht.json file and create a list of nodes
    with open('hash_generator/dht.json', encoding="utf-8") as file1:
        nodes = json.load(file1)
    dht = nodes['dht']
    node_list = []
    hash_to_id ={}
    for node in dht:
        node_list.append((node['hash'], node['id'], node['IP'], node['port']))
        hash_to_id[node['hash']] = (node['id'])
    node_list.sort()

    #[3,7,11,13]
    #8
    for hahs, id_, ip, port in node_list:
        if hash_val <= hahs:
            return id_, ip, port
    return node_list[0][1], node_list[0][2], node_list[0][3]

def get_random_disc_node():
    '''This function returns a random node from the DHT'''
    with open('hash_generator/dht.json', encoding="utf-8") as file1:
        nodes = json.load(file1) 
    dht = nodes['dht']
    random_node = random.choice(dht)
    return random_node['IP']+':'+str(random_node['port'])