"""Ryu controller for the Distributed Backup System.

SDN_MODE=dynamic (default): select server+path per backup session, react to link failures
                            (reroute) and congestion (rebalance), log every decision.
SDN_MODE=static           : baseline. Everything pinned to path A / server h3, no reaction.

Run:  SDN_MODE=dynamic ryu-manager controller/controller.py
REST (port 8080):  GET /select?client=<ip>   POST /event {event,client,server}   GET /status
"""
import json
import os
import time

from ryu.app.wsgi import ControllerBase, WSGIApplication, route
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import DEAD_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib import hub
from ryu.ofproto import ofproto_v1_3
from webob import Response

APP = "backup_app"
MODE = os.environ.get("SDN_MODE", "dynamic").lower()
LINK_MBPS = float(os.environ.get("LINK_MBPS", "10"))
CONG_FRAC = float(os.environ.get("CONG_FRAC", "0.8"))      # congested above 80% of link rate
STATS_INTERVAL = 2.0
COOLDOWN = 6.0
EVENT_LOG = os.environ.get("EVENT_LOG", "results/controller_events.jsonl")

CLIENT_PORT = {"10.0.0.1": 1, "10.0.0.2": 2}               # port on s1
SERVER_PORT = {"10.0.0.3": 1, "10.0.0.4": 2}               # port on s4
SERVERS = list(SERVER_PORT)
SERVICE_PORT = 9000
# (dpid, port) -> logical link; used to translate OpenFlow port-status into link state
PORT_LINK = {(1, 3): "A1", (2, 1): "A1", (2, 2): "A2", (4, 3): "A2",
             (1, 4): "B1", (3, 1): "B1", (3, 2): "B2", (4, 4): "B2"}
PATH_LINKS = {"A": ("A1", "A2"), "B": ("B1", "B2")}


class BackupController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {"wsgi": WSGIApplication}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        kwargs["wsgi"].register(Rest, {APP: self})
        self.dps = {}
        self.down_ports = set()
        self.assign = {}                      # client_ip -> {"server","path"}
        self.suspect = {}                     # server_ip -> time until considered healthy again
        self.util = {"A": 0.0, "B": 0.0}      # Mbps measured on s1 uplinks
        self.prev_tx = {}
        self.last_move = 0.0
        os.makedirs(os.path.dirname(EVENT_LOG) or ".", exist_ok=True)
        hub.spawn(self._stats_loop)
        self.logger.info("controller started, mode=%s", MODE)

    # ------------------------------------------------------------ logging
    def log_event(self, kind, **kw):
        rec = dict(t=round(time.time(), 3), mode=MODE, event=kind, **kw)
        self.logger.info("EVENT %s", json.dumps(rec))
        with open(EVENT_LOG, "a") as f:
            f.write(json.dumps(rec) + "\n")

    # ------------------------------------------------------------ switches
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def on_state(self, ev):
        dp = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            self.dps[dp.id] = dp
            if len(self.dps) == 4:
                self.install_defaults()
        else:
            self.dps.pop(dp.id, None)

    def add_flow(self, dpid, src, dst, out_port):
        dp = self.dps[dpid]
        ofp, ps = dp.ofproto, dp.ofproto_parser
        match = ps.OFPMatch(eth_type=0x0800, ipv4_src=src, ipv4_dst=dst)
        inst = [ps.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, [ps.OFPActionOutput(out_port)])]
        dp.send_msg(ps.OFPFlowMod(datapath=dp, priority=100, match=match, instructions=inst))

    def install_route(self, client, server, path):
        """Install forward+reverse flows client<->server along path A (via s2) or B (via s3)."""
        mid, up = (2, 3) if path == "A" else (3, 4)
        cport, sport = CLIENT_PORT[client], SERVER_PORT[server]
        for dpid, out in ((1, up), (mid, 2), (4, sport)):
            self.add_flow(dpid, client, server, out)
        for dpid, out in ((4, up), (mid, 1), (1, cport)):
            self.add_flow(dpid, server, client, out)

    def install_defaults(self):
        for c in CLIENT_PORT:
            for s in SERVER_PORT:
                a = self.assign.get(c)
                path = a["path"] if a and a["server"] == s else self.first_live() or "A"
                self.install_route(c, s, path)

    # ------------------------------------------------------------ link state
    def path_live(self, p):
        down = {PORT_LINK[k] for k in self.down_ports}
        return not any(l in down for l in PATH_LINKS[p])

    def first_live(self):
        return next((p for p in "AB" if self.path_live(p)), None)

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def on_port_status(self, ev):
        msg = ev.msg
        dp, port = msg.datapath, msg.desc
        key = (dp.id, port.port_no)
        if key not in PORT_LINK:
            return
        ofp = dp.ofproto
        is_down = bool(port.state & ofp.OFPPS_LINK_DOWN) or bool(port.config & ofp.OFPPC_PORT_DOWN)
        (self.down_ports.add if is_down else self.down_ports.discard)(key)
        self.log_event("port_down" if is_down else "port_up", dpid=dp.id, port=port.port_no,
                       link=PORT_LINK[key], live_paths=[p for p in "AB" if self.path_live(p)])
        if MODE == "dynamic":
            self.reconcile("link-event")

    def reconcile(self, why):
        for client, a in list(self.assign.items()):
            if not self.path_live(a["path"]):
                alt = self.first_live()
                if alt:
                    self.move(client, alt, why)
        self.install_defaults()

    def move(self, client, new_path, why):
        a = self.assign[client]
        self.log_event("reroute", client=client, server=a["server"], old=a["path"], new=new_path, why=why)
        a["path"] = new_path
        self.install_route(client, a["server"], new_path)
        self.last_move = time.time()

    # ------------------------------------------------------------ monitoring
    def _stats_loop(self):
        while True:
            hub.sleep(STATS_INTERVAL)
            dp = self.dps.get(1)
            if dp:
                dp.send_msg(dp.ofproto_parser.OFPPortStatsRequest(dp, 0, dp.ofproto.OFPP_ANY))

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def on_stats(self, ev):
        if ev.msg.datapath.id != 1:
            return
        now = time.time()
        for st in ev.msg.body:
            if st.port_no in (3, 4):                       # s1 uplinks: 3 -> path A, 4 -> path B
                p = "A" if st.port_no == 3 else "B"
                prev = self.prev_tx.get(p)
                if prev:                                    # client->server direction only
                    self.util[p] = max(0.0, (st.tx_bytes - prev[0]) * 8 / 1e6 / (now - prev[1]))
                self.prev_tx[p] = (st.tx_bytes, now)
        if MODE == "dynamic":
            self.rebalance()

    def rebalance(self):
        if time.time() - self.last_move < COOLDOWN:
            return
        for client, a in self.assign.items():
            p = a["path"]
            o = "B" if p == "A" else "A"
            if (self.util[p] > CONG_FRAC * LINK_MBPS and self.path_live(o)
                    and self.util[o] < 0.5 * self.util[p]):
                self.log_event("congestion", path=p, util=self.util.copy())
                self.move(client, o, "congestion")
                return

    # ------------------------------------------------------------ REST logic
    def select(self, client):
        if client not in CLIENT_PORT:
            return {"error": "unknown client"}
        if MODE == "static":
            server, path = SERVERS[0], "A"
        else:
            live = [p for p in "AB" if self.path_live(p)]
            if not live:
                return {"error": "no live path"}
            now = time.time()
            healthy = [s for s in SERVERS if self.suspect.get(s, 0) < now] or SERVERS
            load = {s: sum(1 for a in self.assign.values() if a["server"] == s) for s in SERVERS}
            server = min(healthy, key=lambda s: (load[s], s))
            cnt = {p: sum(1 for a in self.assign.values() if a["path"] == p) for p in "AB"}
            path = min(live, key=lambda p: (self.util[p] + 0.25 * LINK_MBPS * cnt[p], p))
        self.assign[client] = {"server": server, "path": path}
        self.install_route(client, server, path)
        self.log_event("select", client=client, server=server, path=path, util=self.util.copy())
        return {"server": server, "port": SERVICE_PORT, "path": path}

    def app_event(self, ev):
        c, s, kind = ev.get("client"), ev.get("server"), ev.get("event")
        self.log_event("app_" + str(kind), client=c, server=s)
        if kind == "finished":
            self.assign.pop(c, None)
        elif kind == "failed" and s:
            self.suspect[s] = time.time() + 15            # steer new sessions away briefly


class Rest(ControllerBase):
    def __init__(self, req, link, data, **config):
        super().__init__(req, link, data, **config)
        self.app = data[APP]

    def _json(self, obj):
        return Response(content_type="application/json", body=json.dumps(obj).encode())

    @route("backup", "/select", methods=["GET"])
    def select(self, req, **kw):
        return self._json(self.app.select(req.params.get("client", "")))

    @route("backup", "/event", methods=["POST"])
    def event(self, req, **kw):
        self.app.app_event(json.loads(req.body or b"{}"))
        return self._json({"ok": True})

    @route("backup", "/status", methods=["GET"])
    def status(self, req, **kw):
        a = self.app
        return self._json({"mode": MODE, "util_mbps": a.util, "assignments": a.assign,
                           "down_ports": sorted(map(list, a.down_ports))})
