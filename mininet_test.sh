h1 python3 DiscoveryAppln.py -n disc5  -p 5555 > disc5.out 2>&1 &
h1 python3 DiscoveryAppln.py -n disc14  -p 5556 > disc14.out 2>&1 &
h2 python3 DiscoveryAppln.py -n disc11  -p 5555 > disc11.out 2>&1 &
h4 python3 DiscoveryAppln.py -n disc13  -p 5555 > disc13.out 2>&1 &
h5 python3 DiscoveryAppln.py -n disc19  -p 5555 > disc19.out 2>&1 &
h6 python3 DiscoveryAppln.py -n disc2  -p 5555 > disc2.out 2>&1 &
h8 python3 DiscoveryAppln.py -n disc9  -p 5555 > disc9.out 2>&1 &
h8 python3 DiscoveryAppln.py -n disc16  -p 5556 > disc16.out 2>&1 &
h9 python3 DiscoveryAppln.py -n disc4  -p 5555 > disc4.out 2>&1 &
h10 python3 DiscoveryAppln.py -n disc8  -p 5555 > disc8.out 2>&1 &
h10 python3 DiscoveryAppln.py -n disc15  -p 5556 > disc15.out 2>&1 &
h10 python3 DiscoveryAppln.py -n disc18  -p 5557 > disc18.out 2>&1 &
h13 python3 DiscoveryAppln.py -n disc20  -p 5555 > disc20.out 2>&1 &
h14 python3 DiscoveryAppln.py -n disc1  -p 5555 > disc1.out 2>&1 &
h14 python3 DiscoveryAppln.py -n disc3  -p 5556 > disc3.out 2>&1 &
h14 python3 DiscoveryAppln.py -n disc17  -p 5557 > disc17.out 2>&1 &
h15 python3 DiscoveryAppln.py -n disc10  -p 5555 > disc10.out 2>&1 &
h19 python3 DiscoveryAppln.py -n disc6  -p 5555 > disc6.out 2>&1 &
h19 python3 DiscoveryAppln.py -n disc7  -p 5556 > disc7.out 2>&1 &
h19 python3 DiscoveryAppln.py -n disc12  -p 5557 > disc12.out 2>&1 &
h12 python3 PublisherAppln.py -n pub3  -a 10.0.0.12 -T 5 > pub3.out 2>&1 &
h24 h24 sleep 15
h13 python3 PublisherAppln.py -n pub1  -a 10.0.0.13 -T 7 > pub1.out 2>&1 &
h24 h24 sleep 15
h13 python3 PublisherAppln.py -n pub4  -a 10.0.0.13 -p 7770 -T 8 > pub4.out 2>&1 &
h24 sleep 15
h14 python3 PublisherAppln.py -n pub2  -a 10.0.0.14 -T 5 > pub2.out 2>&1 &
h24 sleep 15
h15 python3 PublisherAppln.py -n pub5  -a 10.0.0.15 -T 5 > pub5.out 2>&1 &  
h24 sleep 15
h8 python3 SubscriberAppln.py -n sub1 -p 7771  -T 7 > sub1.out 2>&1 &
h24 sleep 15
h8 python3 SubscriberAppln.py -n sub2  -T 5 > sub2.out 2>&1 &
h24 sleep 15
h10 python3 SubscriberAppln.py -n sub3  -T 9 > sub3.out 2>&1 &
h24 sleep 15
h17 python3 SubscriberAppln.py -n sub4 -p 7772 -T 7 > sub4.out 2>&1 &
h24 sleep 15
h17 python3 SubscriberAppln.py -n sub5  -T 7 > sub5.out 2>&1 &
h24 sleep 15
