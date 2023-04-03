import zmq
import json
with open('discovery_pubs.json') as user_file:
    file_contents = user_file.read()
discovery_nodes = json.loads(file_contents)
context = zmq.Context()
sub = context.socket(zmq.SUB)
for value in discovery_nodes.values():

    print(f'tcp://{value}')
    sub.connect(f'tcp://{value}')

sub.setsockopt_string(zmq.SUBSCRIBE,"leader")
sub.recv_string()
# poller.register(self.sub, zmq.POLLIN)