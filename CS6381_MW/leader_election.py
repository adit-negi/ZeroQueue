from datetime import datetime
import time
import argparse
from kazoo.client import KazooClient



class ApplicationNode(object):

    def __init__(self, server_name, server_data, chroot, zookeeper_hosts=''):
        print(zookeeper_hosts)
        self.zookeeper = KazooClient(hosts=zookeeper_hosts)

        self.server_name = server_name
        self.server_data = server_data

        self.patch_chroot = chroot
        self.path_nodes = "/nodes"
        self.path_data = "/data"

        self.connect()
        self.chroot()
        self.register()
        self.watch_application_nodes()
        self.watch_application_data()

    def connect(self):
        print("trying to connect")
        self.zookeeper.start()
        print('done connecting')

    def chroot(self):
        self.zookeeper.ensure_path(self.patch_chroot)
        self.zookeeper.chroot = self.patch_chroot

    def register(self):
        self.zookeeper.create("{0}/{1}_".format(self.path_nodes, self.server_name),
                              ephemeral=True, sequence=True, makepath=True)

    def watch_application_data(self):
        self.zookeeper.ensure_path(self.path_data)
        self.zookeeper.DataWatch(path=self.path_data, func=self.check_application_data)

    def watch_application_nodes(self):
        self.zookeeper.ensure_path(self.path_nodes)
        self.zookeeper.ChildrenWatch(path=self.path_nodes, func=self.check_application_nodes)

    def check_application_nodes(self, children):
        application_nodes = [{"node": i[0], "sequence": i[1]} for i in (i.split("_") for i in children)]
        current_leader = min(application_nodes, key=lambda x: x["sequence"])["node"]

        self.display_server_information(application_nodes, current_leader)
        if current_leader == self.server_name:
            print("I AM THE NEW LEADER")
            self.update_shared_data()

    def check_application_data(self, data, stat):
        print(
            "Data change detected on {0}:\nData: {1}\nStat: {2}".format((datetime.now()).strftime("%B %d, %Y %H:%M:%S"),
                                                                        data, stat))
        print()

    def update_shared_data(self):
        if not self.zookeeper.exists(self.path_data):
            self.zookeeper.create(self.path_data,
                                  bytes("name: {0}\ndata: {1}".format(self.server_name, self.server_data), "utf8"),
                                  ephemeral=True, sequence=False, makepath=True)

    def display_server_information(self, application_nodes, current_leader):
        print("Datetime: {0}".format((datetime.now()).strftime("%B %d, %Y %H:%M:%S")))
        print("Server name: {0}".format(self.server_name))
        print("Nodes:")
        for i in application_nodes:
            print("  - {0} with sequence {1}".format(i["node"], i["sequence"]))
        print("Role: {0}".format("leader" if current_leader == self.server_name else "follower"))
        print()

    def __del__(self):
        self.zookeeper.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ZooKeeper example application')
    parser.add_argument('--server')
    parser.add_argument('--data')
    parser.add_argument('--chroot')
    parser.add_argument('--zookeeper')

    args = parser.parse_args()

    ApplicationNode(server_name=args.server, server_data=args.data, chroot=args.chroot, zookeeper_hosts=args.zookeeper)

    while True:
        time.sleep(10)