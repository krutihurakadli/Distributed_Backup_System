#!/usr/bin/env python3
"""Diamond topology for the Distributed Backup System (run with: sudo python3 topology/topo.py)

            +--- s2 ---+                 h1 10.0.0.1  client
   h1,h2 -- s1          s4 -- h3,h4      h2 10.0.0.2  client / background traffic
            +--- s3 ---+                 h3 10.0.0.3  backup server 1
                                         h4 10.0.0.4  backup server 2
   Path A = s1-s2-s4, Path B = s1-s3-s4.  Switch dpid == switch number (s1 -> dpid 1).

Port map (the controller relies on it):
   s1: 1=h1 2=h2 3=s2 4=s3      s2: 1=s1 2=s4      s3: 1=s1 2=s4
   s4: 1=h3 2=h4 3=s2 4=s3

Management network (192.168.100.0/24): every host gets a 2nd interface to a standalone
switch s0 that also connects to the VM itself (192.168.100.254). Apps use it to reach the
controller's REST API, so control traffic never touches the data-plane links under test.
"""
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.node import Node, OVSBridge, OVSSwitch, RemoteController

MGMT = "192.168.100"
ROOT_IP = MGMT + ".254"
CONTROLLER_URL = "http://%s:8080" % ROOT_IP


def build_net(bw=10, delay="5ms", controller_ip="127.0.0.1", controller_port=6653):
    net = Mininet(controller=None, switch=OVSSwitch, link=TCLink, autoSetMacs=True)
    net.addController("c0", controller=RemoteController, ip=controller_ip, port=controller_port)

    s = {n: net.addSwitch(n, protocols="OpenFlow13") for n in ("s1", "s2", "s3", "s4")}
    s0 = net.addSwitch("s0", cls=OVSBridge)                    # standalone management switch
    h = {i: net.addHost("h%d" % i, ip="10.0.0.%d/24" % i) for i in (1, 2, 3, 4)}

    # access links (fast), explicit port numbers
    net.addLink(h[1], s["s1"], port2=1)
    net.addLink(h[2], s["s1"], port2=2)
    net.addLink(h[3], s["s4"], port2=1)
    net.addLink(h[4], s["s4"], port2=2)
    # core links (bottlenecks): the two alternative paths
    core = dict(bw=bw, delay=delay)
    net.addLink(s["s1"], s["s2"], port1=3, port2=1, **core)
    net.addLink(s["s2"], s["s4"], port1=2, port2=3, **core)
    net.addLink(s["s1"], s["s3"], port1=4, port2=1, **core)
    net.addLink(s["s3"], s["s4"], port1=2, port2=4, **core)

    # management network
    for i in (1, 2, 3, 4):
        net.addLink(h[i], s0)
    root = Node("root", inNamespace=False)
    root_intf = net.addLink(root, s0).intf1
    root.setIP(ROOT_IP + "/24", intf=root_intf)

    net.start()
    for i in (1, 2, 3, 4):
        h[i].setIP("%s.%d/24" % (MGMT, i), intf="h%d-eth1" % i)
    net.staticArp()                                            # no ARP flooding -> no loops
    return net


if __name__ == "__main__":
    setLogLevel("info")
    net = build_net()
    print("\nController REST for hosts: %s" % CONTROLLER_URL)
    print("Try: h1 ping -c2 h3   |   link s2 s4 down   |   sh ovs-ofctl -O OpenFlow13 dump-flows s1\n")
    CLI(net)
    net.stop()
