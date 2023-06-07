# ZeroQueue

Welcome to the ZeroQueue! This project implements a pub/sub model using ZeroMQ sockets as the underlying communication mechanism. It also incorporates a discovery middleware that efficiently tracks topics, publishers, subscribers' addresses, and IP information. Additionally, a broker service acts as an intermediary for message exchange, enhancing the overall communication flow.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Usage](#usage)
- [Architecture](#architecture)
- [Features](#features)
- [Fault Tolerance](#fault-tolerance)
- [Contributing](#contributing)
- [License](#license)

## Overview

Welcome to the ZeroQueue! This project implements a pub/sub model using ZeroMQ sockets as the underlying communication mechanism. It also incorporates a discovery middleware that efficiently tracks topics, publishers, subscribers' addresses, and IP information. Additionally, a broker service acts as an intermediary for message exchange, enhancing the overall communication flow.

## Installation

Step-by-step instructions on how to install and set up the project. Include any dependencies and requirements. 

```shell
$ git clone git@github.com:adit-negi/ZeroQueue.git
$ cd ZeroQueue
$ pip install -r requirements.txt
```

## Usage
Install mininet to directly run the mininet_test.sh script or you can remove the hostnames from the script and change port numbers to run it on bash shell
```shell
$ ./mininet_test.sh
```

## Architecture

The technical architecture of the project is designed to provide a robust and scalable pub/sub messaging system using ZeroMQ sockets, a discovery middleware, and ZooKeeper for fault tolerance. The architecture ensures efficient message routing, load balancing, and high availability. 

Unstable funcationality for a the discovery nodes to be in Distributed hash table is also supported. Chord algorithm is used to power it.


### Components:
Publishers: Publishers are responsible for sending messages to specific topics. They use ZeroMQ sockets to establish connections with the broker service and publish messages.

1. Subscribers: Subscribers express their interest in receiving messages from specific topics. They also utilize ZeroMQ sockets to connect with the broker service and receive messages.

2. Discovery Middleware: The discovery middleware acts as a central repository for storing and managing topic information, including publishers' and subscribers' addresses, IP information, and other metadata. It efficiently tracks and updates the information to facilitate seamless communication between publishers and subscribers.

3. Broker Service: The broker service serves as an intermediary for message exchange. It receives published messages from publishers, processes them, and forwards them to relevant subscribers based on their subscriptions and topic interests. The broker service ensures reliable delivery and efficient message routing.

4. ZooKeeper: ZooKeeper is integrated into the architecture to provide fault tolerance and high availability. It handles node failures, leader election, and maintains consistent states across the system. ZooKeeper ensures that the pub/sub system can gracefully recover from failures without compromising message delivery.

5. Consistent Hashing via Chord: All discovery nodes are in form of a dht ring, to communicate with each other Chord algorithm is leveraged.

### Message Flow:
1. Publishers publish messages to specific topics through ZeroMQ sockets. They establish connections with the broker service and send messages.

2. The broker service receives the published messages and stores them temporarily. It then uses the discovery middleware to identify interested subscribers based on topic subscriptions.

3. The broker service forwards the messages to the relevant subscribers via ZeroMQ sockets. It ensures that each subscriber receives the message only if it is subscribed to the associated topic.

4. Subscribers receive the messages through ZeroMQ sockets. They process the messages based on their subscribed topics and perform the necessary actions.

### Fault Tolerance:
To achieve fault tolerance, the project leverages ZooKeeper. ZooKeeper provides the following benefits:

1. Node Failure Handling: ZooKeeper detects node failures and initiates the recovery process. It ensures that the system continues functioning even if some nodes become unavailable.

2. Leader Election: ZooKeeper facilitates leader election to maintain consistency and coordination within the system. It ensures that a single node acts as the leader and handles message distribution and coordination tasks.

3. Consistent States: ZooKeeper maintains consistent states across all nodes, ensuring that the pub/sub system operates reliably and without inconsistencies.

By incorporating ZooKeeper into the architecture, the project achieves high availability, fault tolerance, and the ability to recover gracefully from failures, ensuring uninterrupted message delivery and system operation.

This technical architecture provides a solid foundation for building a scalable, reliable, and fault-tolerant pub/sub messaging system. It enables efficient message routing, load balancing, and seamless communication between publishers and subscribers, enhancing the overall messaging experience.

## Features
This project offers a range of features to enhance the pub/sub experience:

1. Load Balancing: Distributes message load evenly across multiple subscribers, ensuring optimal resource utilization. 

2. Ownership Strength: Allows publishers to control the strength of their ownership over topics, providing flexibility in message distribution.

3. Dead Letter Queues: Provides a mechanism to handle failed message delivery by storing them in dedicated queues for later processing.

4. Warm Passive Fault Tolerance: Ensures high availability by implementing warm standby nodes that can take over in case of failures.
5. Quality of Service (QoS): Supports different levels of QoS to prioritize messages based on their criticality and urgency.

6. History: Logs message history to enable auditing, analysis, and replay of past communication.

## Contributing
No rules -- PRs are welcome