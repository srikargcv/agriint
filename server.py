#!/usr/bin/env python3
"""AgriNexus — Intern Edition.

Self-contained demo of the AgriNexus ecosystem-intelligence pipeline:
matching, pricing, logistics, and trust/fraud on a shared event stream.

- Python standard library ONLY (no pip installs needed)
- Small dummy dataset (dummy_data.json / dummy_mandi.json / dummy_towns.json)
- Same web UI as the production prototype

Run:  python3 server.py        →  http://localhost:8765
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import statistics
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

ROOT = Path(__file__).resolve().parent
D = json.loads((ROOT / "dummy_data.json").read_text())
MANDI = json.loads((ROOT / "dummy_mandi.json").read_text())
TOWNS = json.loads((ROOT / "dummy_towns.json").read_text())

BAND_HALF_WIDTH = 0.06
LOGISTICS_COST = 60.0
COMMISSION = 0.02
MATCH_THRESHOLD = 0.55
TRUST_DECAY, TRUST_ALPHA = 0.02, 0.25
SPOILAGE_REF_MIN = 240.0

EVENT_LOG: list[dict] = []


def publish(etype: str, payload: dict):
    EVENT_LOG.append({"type": etype, "ts": round(time.time(), 3), "payload": payload})


# ---------------------------------------------------------------- helpers

def haversine_km(a, b) -> float:
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def mandi_ref(crop: str, district: str):
    alias = D["alias"].get(crop, crop)
    rec = MANDI.get(district, {}).get("rates", {}).get(alias)
    if rec:
        return float(rec["modal"]), rec
    return float(D["fallback_rates"].get(district, {}).get(crop, 2200)), None


def mandi_history(crop: str, district: str, days: int = 21, seed: int = 1) -> list[float]:
    ref, _ = mandi_ref(crop, district)
    rng = random.Random(hash((crop, district, seed)) & 0xFFFF)
    return [round(ref * (1 + (0.04 * math.sin(2 * math.pi * d / 7) if d else 0) + rng.uniform(-0.02, 0.02)), 0)
            for d in range(days - 1, -1, -1)]


def hub_coords(district: str):
    for t in TOWNS:
        if t["name"].lower() == district.lower():
            return (t["lat"], t["lng"])
    return (16.3, 80.4)


# ---------------------------------------------------------------- data

def make_lots(seed: int = 42):
    rng = random.Random(seed)
    samples = [("A", "harvested this morning, firm texture, no blemishes"),
               ("B", "harvested yesterday, good condition, minor surface marks"),
               ("C", "harvested 3 days ago, visible wilting on some units")]
    lots, n = [], 0
    for fid, _, village, district, lat, lng, crops, kyc in D["farmers"]:
        for crop in crops:
            n += 1
            grade, desc = samples[rng.randrange(3)]
            ref, _ = mandi_ref(crop, district)
            lots.append({"lot_id": f"LOT-{n:03d}", "farmer_id": fid, "village": village,
                         "district": district, "crop": crop,
                         "quantity_q": round(rng.uniform(20, 120), 1), "grade": grade,
                         "freshness_desc": desc, "lat": lat, "lng": lng, "kyc": kyc,
                         "perishable": crop in D["perishables"],
                         "list_price": round(ref * D["grades"][grade] * rng.uniform(0.94, 1.10))})
    return lots


def make_orders(seed: int = 7):
    rng = random.Random(seed)
    buyers = [
        ("B-RET-1", "Vijayawada Retail Co-op", "retail", "Krishna", ("Paddy", "Tomato", "Banana")),
        ("B-RET-2", "Tenali Fresh Mart", "retail", "Guntur", ("Paddy", "Chili", "Tomato")),
        ("B-HOT-1", "Amaravati HoReCa Union", "horeca", "Guntur", ("Tomato", "Chili", "Paddy")),
        ("B-EXP-1", "Kakinada Export House", "export", "East Godavari", ("Paddy", "Coconut", "Cotton")),
        ("B-HOT-2", "Nellore Canteen Services", "horeca", "Nellore", ("Cotton", "Groundnut", "Paddy")),
        ("B-EXP-2", "Krishna Agri Exports", "export", "Krishna", ("Paddy", "Wheat", "Maize")),
    ]
    orders, n = [], 0
    for bid, name, cls, district, crops in buyers:
        for crop in crops:
            n += 1
            ref, _ = mandi_ref(crop, district)
            orders.append({"order_id": f"ORD-{n:03d}", "buyer_id": bid, "buyer_name": name,
                           "stakeholder_class": cls, "district": district, "crop": crop,
                           "quantity_q": round(rng.uniform(15, 100), 1),
                           "max_price": round(ref * rng.uniform(1.02, 1.15)),
                           "hub": hub_coords(district),
                           "min_trust": 0.35 if cls == "retail" else (0.55 if cls == "horeca" else 0.70)})
    return orders


def base_data():
    farmers = {f[0]: {"name": f[1], "village": f[2], "district": f[3], "kyc": f[7], "crops": f[6]}
               for f in D["farmers"]}
    trust = {f[0]: (0.8 if f[7] == "Verified" else 0.4) for f in D["farmers"]}
    return farmers, trust


# ---------------------------------------------------------------- matching

def _price_fit(lot, order):
    return 1.0 if lot["list_price"] <= order["max_price"] else max(0.0, 1 - (lot["list_price"] - order["max_price"]) / order["max_price"])


def _freshness(lot):
    base = D["freshness_base"][lot["grade"]]
    text = lot["freshness_desc"].lower()
    if any(w in text for w in ("wilting", "blemish", "spoiled")):
        base -= 0.15
    if any(w in text for w in ("firm", "fresh", "morning")):
        base += 0.05
    return min(1.0, max(0.0, base))


def _distance(lot, order, ref=120.0):
    return max(0.0, 1.0 - haversine_km((lot["lat"], lot["lng"]), order["hub"]) / ref)


def _capacity(lot, order):
    return min(1.0, lot["quantity_q"] / max(order["quantity_q"], 1e-9))


def score(lot, order, trust_lookup):
    w = D["weights"][order["stakeholder_class"]]
    subs = {"price_fit": _price_fit(lot, order), "freshness": _freshness(lot),
            "distance": _distance(lot, order), "trust": trust_lookup.get(lot["farmer_id"], 0.5),
            "capacity": _capacity(lot, order)}
    return round(sum(w[k] * v for k, v in subs.items()), 4), {k: round(v, 3) for k, v in subs.items()}


def match_all(lots, orders, trust_lookup):
    """Greedy assignment: sort every eligible pair by score, take best-first."""
    pairs = []
    for l in lots:
        for o in orders:
            if l["crop"] != o["crop"] or l["district"] != o["district"]:
                continue
            if l["quantity_q"] < 0.5 * o["quantity_q"]:
                continue
            if trust_lookup.get(l["farmer_id"], 0.5) < o["min_trust"]:
                continue
            s, subs = score(l, o, trust_lookup)
            if s >= MATCH_THRESHOLD:
                pairs.append({"lot_id": l["lot_id"], "order_id": o["order_id"], "score": s, "subsignals": subs})
    pairs.sort(key=lambda m: -m["score"])
    used_l, used_o, out = set(), set(), []
    for m in pairs:
        if m["lot_id"] in used_l or m["order_id"] in used_o:
            continue
        used_l.add(m["lot_id"]); used_o.add(m["order_id"]); out.append(m)
    return out


# ---------------------------------------------------------------- pricing

def fair_band(crop, district, grade="B", rates=None):
    hist = rates or mandi_history(crop, district, days=7)
    ref = statistics.mean(hist[-3:])
    premium = D["grades"].get(grade, 1.0)
    floor = ref * premium * (1 - BAND_HALF_WIDTH) + LOGISTICS_COST
    ceiling = ref * premium * (1 + BAND_HALF_WIDTH) + LOGISTICS_COST
    return {"crop": crop, "district": district, "grade": grade, "mandi_ref": round(ref, 0),
            "floor_gross": round(floor, 0), "ceiling_gross": round(ceiling, 0),
            "floor_net": round(floor * (1 - COMMISSION), 0), "ceiling_net": round(ceiling * (1 - COMMISSION), 0)}


def forecast(crop, district, horizon=5):
    hist = mandi_history(crop, district, days=28)
    base = statistics.mean(hist[-7:])
    day_idx = len(hist) % 7
    out = []
    for h in range(1, horizon + 1):
        seasonal = statistics.mean(x for i, x in enumerate(hist) if i % 7 == (day_idx + h) % 7) / statistics.mean(hist)
        out.append(round(base * seasonal, 0))
    return out


def explain_band(band, fc_next):
    trend = "rising" if fc_next > band["mandi_ref"] else "softening"
    return (f"For {band['grade']}-grade {band['crop']} in {band['district']}, today's mandi reference is "
            f"₹{band['mandi_ref']:.0f}/q. Your fair band is ₹{band['floor_gross']:.0f}–₹{band['ceiling_gross']:.0f}/q "
            f"(including ₹60/q logistics; you net ₹{band['floor_net']:.0f}–₹{band['ceiling_net']:.0f} after the 2% "
            f"platform fee). The 5-day forecast suggests prices are {trend}.")


# ---------------------------------------------------------------- logistics

def spoilage_risk(perishable, transit_min, max_temp=28.0):
    if not perishable:
        return 0.0
    time_over = max(0.0, transit_min - SPOILAGE_REF_MIN) / SPOILAGE_REF_MIN
    temp_over = max(0.0, max_temp - 28.0) / 10.0
    return min(1.0, 0.6 * time_over + 0.4 * temp_over)


def choose_vehicle(route, cargo_q, max_temp):
    perish = sum(1 for s in route if s.get("perishable")) / len(route)
    fleet = D["vehicles"]
    if perish >= 0.5 and max_temp > 26.0:
        for v in fleet:
            if v["cold"] and v["payload_q"] >= cargo_q:
                return v["key"]
    for v in fleet:
        if not v["cold"] and v["payload_q"] >= cargo_q:
            return v["key"]
    return max(fleet, key=lambda v: v["payload_q"])["key"]


def solve_route(stops, depot, vehicle_key, max_temp=28.0):
    """Nearest-neighbour tour with a spoilage-weighted cost; stdlib substitute
    for the OR-Tools CVRPTW solver used in the production prototype."""
    v = next(x for x in D["vehicles"] if x["key"] == vehicle_key)
    remaining = list(stops)
    tour, pos = [], depot
    while remaining:
        def arc_cost(s):
            km = haversine_km(pos, (s["lat"], s["lng"]))
            mins = km / v["speed_kmh"] * 60
            spoil = spoilage_risk(s.get("perishable", False), mins, max_temp)
            return km * v["cost_km"] * (1 + spoil)
        nxt = min(remaining, key=arc_cost)
        tour.append(nxt); pos = (nxt["lat"], nxt["lng"]); remaining.remove(nxt)
    total_km = (sum(haversine_km((tour[i - 1]["lat"], tour[i - 1]["lng"]), (tour[i]["lat"], tour[i]["lng"]))
                    for i in range(1, len(tour)))
                + haversine_km(depot, (tour[0]["lat"], tour[0]["lng"]))
                + haversine_km((tour[-1]["lat"], tour[-1]["lng"]), depot)) if tour else 0.0
    transit = total_km / v["speed_kmh"] * 60
    risk = max((spoilage_risk(s.get("perishable", False), transit, max_temp) for s in tour), default=0.0)
    return {"vehicle": vehicle_key, "stops": [s["lot_id"] for s in tour],
            "distance_km": round(total_km, 1), "transit_min": round(transit, 0),
            "spoilage_risk": round(risk, 3), "needs_reroute": risk > 0.4}


# ---------------------------------------------------------------- trust & fraud

def update_trust(current, outcome, alpha=TRUST_ALPHA):
    decayed = current + TRUST_DECAY * (0.5 - current)
    return round(min(1.0, max(0.0, decayed + alpha * (outcome - decayed))), 4)


STAGES = ["sowing", "harvest", "pickup", "cold_chain", "delivery"]


def append_ledger(chain, lot_id, stage, data):
    prev = chain[-1]["hash"] if chain else "GENESIS"
    rec = {"lot_id": lot_id, "stage": stage, "ts": round(time.time(), 3),
           "prev_hash": prev, "data": data}
    rec["hash"] = hashlib.sha256(json.dumps({k: v for k, v in rec.items()}, sort_keys=True).encode()).hexdigest()[:16]
    chain.append(rec)
    return rec


def fraud_checks(bus_publish=True):
    flags = []

    def flag(rule, lot_id, detail):
        flags.append({"rule": rule, "lot_id": lot_id, "detail": detail, "action": "FLAG_FOR_REVIEW"})
        publish("fraud.flagged", flags[-1])

    if abs(100 - 88) / 100 > 0.05:
        flag("WEIGHT_MISMATCH", "LOT-901", "pickup 100.0q vs delivery 88.0q")
    peers = mandi_history("Paddy", "Guntur", days=14)
    sd = statistics.pstdev(peers) or 1.0
    z = (3400 - statistics.mean(peers)) / sd
    if abs(z) > 2.5:
        flag("PRICE_OUTLIER", "LOT-902", f"price ₹3400 is z={z:.2f} vs mandi peers")
    cancels = [True, False, True, True, True, False, True, True, True, False]
    if sum(cancels) / len(cancels) > 0.30:
        flag("REPEATED_CANCELLATIONS", "FRM-003", "cancellation rate 70% over last 10 orders")
    if haversine_km((16.3067, 80.4365), (16.9, 80.1)) > 5.0:
        flag("GEOFENCE_INCONSISTENCY", "LOT-903", "GPS 75.1km from claimed farm location")
    # circular trading: cycle FRM-001 -> T-77 -> T-88 -> FRM-001
    graph = {"FRM-001": {"T-77"}, "T-77": {"T-88"}, "T-88": {"FRM-001"}, "FRM-002": {"T-99"}}
    color, cycle = {}, []

    def dfs(node, stack):
        color[node] = "gray"
        for nxt in graph.get(node, ()):
            if color.get(nxt) == "gray":
                return stack + [node, nxt]
            if color.get(nxt) is None:
                r = dfs(nxt, stack + [node])
                if r:
                    return r
        color[node] = "black"
        return None

    for node in graph:
        if color.get(node) is None:
            cycle = dfs(node, []) or cycle
            if cycle:
                break
    if cycle and "FRM-001" in cycle:
        flag("CIRCULAR_TRADING", "FRM-001", "cycle detected: " + " -> ".join(cycle))
    return flags


# ---------------------------------------------------------------- stages

def stage_matching():
    EVENT_LOG.clear()
    farmers, trust = base_data()
    lots, orders = make_lots(), make_orders()
    matches = match_all(lots, orders, trust)
    for m in matches:
        lot = next(l for l in lots if l["lot_id"] == m["lot_id"])
        publish("match.confirmed", {**m, "farmer_id": lot["farmer_id"],
                                    "buyer_id": next(o["buyer_id"] for o in orders if o["order_id"] == m["order_id"])})
    return {"matches": [{**m, "crop": next(l["crop"] for l in lots if l["lot_id"] == m["lot_id"]),
                         "farmer": farmers[next(l["farmer_id"] for l in lots if l["lot_id"] == m["lot_id"])]["name"],
                         "farmer_id": next(l["farmer_id"] for l in lots if l["lot_id"] == m["lot_id"])} for m in matches],
            "lots_total": len(lots), "orders_total": len(orders), "farmers": farmers,
            "events": list(EVENT_LOG)}


def stage_pricing():
    EVENT_LOG.clear()
    priced = {}
    for lot in make_lots():
        band = fair_band(lot["crop"], lot["district"], lot["grade"])
        fc = forecast(lot["crop"], lot["district"])
        band["forecast_d5"] = fc
        publish("price.banded", {"lot_id": lot["lot_id"], **band})
        offer = lot["list_price"]
        if offer < band["floor_gross"]:
            publish("price.alert", {"lot_id": lot["lot_id"], "alert": "BELOW_FLOOR", "offer_price": offer})
        if offer > band["ceiling_gross"]:
            publish("price.alert", {"lot_id": lot["lot_id"], "alert": "ABOVE_CEILING", "offer_price": offer})
        band["explanation"] = explain_band(band, fc[-1])
        band["offer_price"] = offer
        band["within_band"] = band["floor_gross"] <= offer <= band["ceiling_gross"]
        priced[lot["lot_id"]] = {**band, "crop": lot["crop"], "farmer_id": lot["farmer_id"]}
    return {"priced": priced, "events": list(EVENT_LOG)}


def stage_logistics():
    EVENT_LOG.clear()
    farmers, trust = base_data()
    lots, orders = make_lots(), make_orders()
    matches = match_all(lots, orders, trust)
    hubs = {}
    for m in matches:
        lot = next(l for l in lots if l["lot_id"] == m["lot_id"])
        order = next(o for o in orders if o["order_id"] == m["order_id"])
        hubs.setdefault(order["district"], []).append(lot)
    plans = []
    for district, stops in hubs.items():
        cargo = sum(s["quantity_q"] for s in stops)
        vehicle = choose_vehicle(stops, cargo, 30.0)
        plan = solve_route(stops, hub_coords(district), vehicle, 30.0)
        publish("route.planned", {**plan, "district": district})
        if plan["needs_reroute"]:
            fix = {**plan, "vehicle": "reefer", "reroute_reason": "spoilage_risk>0.4"}
            publish("route.rerouted", fix)
            plans.append({**fix, "district": district})
        else:
            plans.append({**plan, "district": district})
    return {"routes": plans, "events": list(EVENT_LOG)}


def stage_trust():
    EVENT_LOG.clear()
    farmers, trust = base_data()
    chain = []
    for s in STAGES:
        rec = append_ledger(chain, "LOT-001", s, {"actor": "system", "ok": True})
        publish("ledger.appended", {k: rec[k] for k in ("lot_id", "stage", "hash", "prev_hash")})

    def verify(c):
        prev = "GENESIS"
        for rec in c:
            if rec["prev_hash"] != prev:
                return False
            check = hashlib.sha256(json.dumps({k: v for k, v in rec.items() if k != "hash"}, sort_keys=True).encode()).hexdigest()[:16]
            if rec["hash"] != check:
                return False
            prev = rec["hash"]
        return True

    fid = "FRM-001"
    before = trust[fid]
    traj = [before]
    for _ in range(3):
        trust[fid] = update_trust(trust[fid], 0.95)
        traj.append(trust[fid])
        publish("trust.updated", {"farmer_id": fid, "new_score": trust[fid], "outcome": 0.95})
    return {"ledger_chain": chain, "ledger_intact": verify(chain),
            "trust_trajectory": {"farmer": farmers[fid]["name"], "values": traj},
            "trust_scores": trust, "farmers": farmers, "events": list(EVENT_LOG)}


def stage_fraud():
    EVENT_LOG.clear()
    flags = fraud_checks()
    publish("settlement.held", {"match_id": "MATCH-1", "reason": "; ".join(f["rule"] for f in flags[:2])})
    return {"flags": flags,
            "escalation": {"match_id": "MATCH-1", "action": "HOLD_SETTLEMENT",
                           "reason": "; ".join(f["rule"] for f in flags[:2])},
            "events": list(EVENT_LOG)}


def custom_run(p: dict):
    EVENT_LOG.clear()
    lot = {"lot_id": "LOT-CUSTOM", "farmer_id": "FRM-CUSTOM", "crop": p["crop"],
           "district": p["district"], "quantity_q": float(p["quantity_q"]),
           "grade": p.get("grade", "FAQ"), "freshness_desc": p.get("freshness_desc", "good condition"),
           "lat": float(p["lat"]), "lng": float(p["lng"]),
           "list_price": float(p["list_price"]),
           "perishable": p["crop"] in D["perishables"]}
    delivery = p.get("delivery_district", p["district"])
    order = {"order_id": "ORD-CUSTOM", "buyer_id": "B-CUSTOM",
             "stakeholder_class": p.get("stakeholder_class", "retail"), "district": delivery,
             "crop": p["crop"], "quantity_q": float(p.get("order_quantity_q", p["quantity_q"])),
             "max_price": float(p["max_price"]), "hub": hub_coords(delivery),
             "min_trust": float(p.get("min_trust", 0))}
    temp = float(p.get("max_temp", 28))
    trust_in = float(p.get("trust_score", 0.5))

    s_val, subs = score(lot, order, {"FRM-CUSTOM": trust_in})
    passed = (lot["quantity_q"] >= 0.5 * order["quantity_q"]
              and trust_in >= order["min_trust"] and s_val >= MATCH_THRESHOLD)
    publish("match.confirmed", {"lot_id": lot["lot_id"], "order_id": "ORD-CUSTOM",
                                "score": s_val, "subsignals": subs, "passed": passed})

    offer = float(p.get("offer_price", lot["list_price"]))
    band = fair_band(p["crop"], p["district"], lot["grade"])
    fc = forecast(p["crop"], p["district"])
    band["forecast_d5"] = fc
    publish("price.banded", {"lot_id": lot["lot_id"], **band})
    band["explanation"] = explain_band(band, fc[-1])
    band["offer_price"] = offer
    band["within_band"] = band["floor_gross"] <= offer <= band["ceiling_gross"]

    veh = p.get("vehicle")
    vehicle = veh if veh and any(v["key"] == veh for v in D["vehicles"]) \
        else choose_vehicle([lot], lot["quantity_q"], temp)
    plan = solve_route([lot], order["hub"], vehicle, temp)
    publish("route.planned", {**plan, "district": delivery, "origin_district": p["district"]})

    outcome = float(p.get("outcome", 0.9))
    trust_after = update_trust(trust_in, outcome)
    publish("trust.updated", {"farmer_id": lot["farmer_id"], "old": trust_in,
                              "new": trust_after, "outcome": outcome})

    flags = []
    peers = mandi_history(p["crop"], p["district"], days=14)
    sd = statistics.pstdev(peers) or 1.0
    z = (offer - statistics.mean(peers)) / sd
    if abs(z) > 2.5:
        flags.append({"rule": "PRICE_OUTLIER", "lot_id": lot["lot_id"],
                      "detail": f"price ₹{offer} is z={z:.2f} vs mandi peers", "action": "FLAG_FOR_REVIEW"})
    if p.get("pickup_q") and p.get("delivery_q") and abs(float(p["pickup_q"]) - float(p["delivery_q"])) / max(float(p["pickup_q"]), 1e-9) > 0.05:
        flags.append({"rule": "WEIGHT_MISMATCH", "lot_id": lot["lot_id"],
                      "detail": f"pickup {p['pickup_q']}q vs delivery {p['delivery_q']}q", "action": "FLAG_FOR_REVIEW"})
    if p.get("gps_lat") and p.get("gps_lng") and haversine_km((lot["lat"], lot["lng"]), (float(p["gps_lat"]), float(p["gps_lng"]))) > 5.0:
        flags.append({"rule": "GEOFENCE_INCONSISTENCY", "lot_id": lot["lot_id"],
                      "detail": "GPS far from claimed farm location", "action": "FLAG_FOR_REVIEW"})

    return {"inputs": {"lot": lot, "order": {k: v for k, v in order.items() if k != "hub"},
                       "offer_price": offer, "max_temp": temp},
            "match": {"score": s_val, "subsignals": subs, "passed": passed},
            "pricing": band,
            "route": {**plan, "district": delivery, "origin_district": p["district"], "vehicle": vehicle},
            "trust": {"before": trust_in, "outcome": outcome, "after": trust_after},
            "fraud": {"flags": flags}, "events": list(EVENT_LOG)}


STAGES_API = {"matching": stage_matching, "pricing": stage_pricing,
              "logistics": stage_logistics, "trust": stage_trust, "fraud": stage_fraud}


# ---------------------------------------------------------------- server

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        if self.path == "/api/run/custom":
            try:
                length = int(self.headers.get("Content-Length", 0))
                payload = json.loads(self.rfile.read(length) or b"{}")
                return self._send(200, custom_run(payload))
            except (KeyError, ValueError) as e:
                return self._send(400, {"error": f"missing or invalid field: {e}"})

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            return self._send(200, (ROOT / "web" / "index.html").read_bytes(), "text/html")
        if self.path.startswith("/api/run/"):
            stage = self.path.rsplit("/", 1)[-1]
            if stage == "all":
                out = {name: fn() for name, fn in STAGES_API.items()}
                out["farmers"] = base_data()[0]
                return self._send(200, out)
            if stage in STAGES_API:
                return self._send(200, STAGES_API[stage]())
            return self._send(404, {"error": f"unknown stage '{stage}'"})
        if self.path == "/api/options":
            districts = []
            for dist, rec in MANDI.items():
                districts.append({"name": dist, "state": rec["state"]})
            for dist, rates in D["fallback_rates"].items():
                if all(d["name"] != dist for d in districts):
                    districts.append({"name": dist, "state": "Andhra Pradesh"})
            return self._send(200, {"crops": D["crops"],
                                    "districts": sorted(districts, key=lambda x: (x["state"], x["name"])),
                                    "grades": list(D["grades"]),
                                    "vehicles": D["vehicles"]})
        if self.path.startswith("/api/cities"):
            q = parse_qs(urlparse(self.path).query).get("q", [""])[0].lower()
            hits = [t for t in TOWNS if q and q in t["name"].lower()]
            hits.sort(key=lambda c: (not c["name"].lower().startswith(q), -c["pop"]))
            return self._send(200, {"cities": hits[:10]})
        if self.path.startswith("/api/mandi"):
            qs = parse_qs(urlparse(self.path).query)
            crop, district = qs.get("crop", [""])[0], qs.get("district", [""])[0]
            if not (crop and district):
                return self._send(400, {"error": "crop and district required"})
            ref, rec = mandi_ref(crop, district)
            return self._send(200, {"crop": crop, "district": district,
                                    "modal_price": ref, "agmarknet": rec})
        if self.path == "/api/health":
            return self._send(200, {"ok": True, "stages": list(STAGES_API) + ["all", "custom"]})
        self._send(404, {"error": "not found"})


if __name__ == "__main__":
    import sys
    requested = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    server = None
    for port in range(requested, requested + 20):
        try:
            server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
            break
        except OSError:
            continue
    if server is None:
        raise SystemExit("no free port found")
    print(f"AgriNexus (Intern Edition) → http://localhost:{port}")
    server.serve_forever()
