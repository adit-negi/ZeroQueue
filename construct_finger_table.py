'''This file constructs the finger table for each node in the DHT'''
import bisect
import json


def construct_finger_table():
    '''This function constructs the finger table for each node in the DHT'''
    # open dht.json file and create a list of nodes
    with open('hash_generator/dht.json', encoding="utf-8") as file1:
        nodes = json.load(file1)
    dht = nodes['dht']
    node_list = []
    hash_to_id ={}
    for node in dht:
        node_list.append(node['hash'])
        hash_to_id[node['hash']] = (node['id'])
    # sort the list of nodes
    node_list.sort()
    # number of bits in hash
    hash_bit_size = 16
    print(node_list)
    master_finger_table = {}
    print(hash_to_id)
    index = 0
    for hash_ in node_list:
        finger_table = {}
        for i in range(hash_bit_size):
            #find appropriate node for each finger
            #using binary search to speed operation
            idx = bisect.bisect_left(node_list, (hash_ + 2**i) % 2**hash_bit_size)


            node_hash = node_list[idx%len(node_list)]

            finger_table[i+1] = {"hash_value": node_hash, "id": hash_to_id[node_hash]}
        #also stores the ithe immediate predecessor
        master_finger_table[hash_to_id[hash_]] = finger_table
        print(hash_to_id[hash_], index, hash_)
        master_finger_table[hash_to_id[hash_]]['predecessor'] = node_list[(index-1)%len(node_list)]
        index+=1
    return master_finger_table

finger_table_json = construct_finger_table()

with open('finger_table.json', 'w', encoding='utf-8') as file:
    json.dump(finger_table_json, file, indent=4)
