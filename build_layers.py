#!/usr/bin/env python3
"""Build the three layers (ods -> dwd -> dws) of the account-flow pipeline.

Resumable: pipeline_state.json tracks per-layer status. Layers marked
"done" are never rebuilt; a layer marked "running" (left half-written by
an interrupted run) is rebuilt from scratch. Each finished layer appends
"<layer> done" to pipeline.log. A rerun with all layers done changes
nothing and adds no log lines.

Usage: python3 -I build_layers.py <base_dir>
"""
import csv
import json
import os
import shutil
import sys
from collections import defaultdict
from decimal import Decimal
from datetime import datetime

BASE = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
STATE_PATH = os.path.join(BASE, "pipeline_state.json")
LOG_PATH = os.path.join(BASE, "pipeline.log")
SRC_FLOW = os.path.join(BASE, "flow.txt")
OPENING = os.path.join(BASE, "opening.csv")
SUMMARY_PATH = os.path.join(BASE, "layer_summary.txt")

LAYERS = ("ods", "dwd", "dws")


def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {layer: "pending" for layer in LAYERS}


def save_state(state):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def log_done(layer):
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write("%s done\n" % layer)


def parse_time(raw):
    # "2026/10/28 15:26:13" or with milliseconds "...15:26:13.000"
    main = raw.split(".", 1)[0]
    dt = datetime.strptime(main, "%Y/%m/%d %H:%M:%S")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_amount(raw):
    # "9,911.84" yuan -> integer cents
    return int(Decimal(raw.replace(",", "")) * 100)


def read_source_rows():
    rows = []
    with open(SRC_FLOW, "r", encoding="utf-8", newline="") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) != 7:
                raise ValueError("bad row (%d fields): %r" % (len(parts), line))
            txn_id, acct, dc, amount, txn_time, post_time, rev_of = parts
            rows.append({
                "txn_id": txn_id,
                "acct": acct,
                "dc": dc.strip().upper(),
                "amount": parse_amount(amount),
                "txn_time": parse_time(txn_time),
                "post_time": parse_time(post_time),
                "rev_of": rev_of.strip(),
            })
    return rows


def build_ods():
    # Byte-for-byte copy of the original file.
    dst = os.path.join(BASE, "ods", "flow.txt")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copyfile(SRC_FLOW, dst)


def build_dwd(rows):
    dst = os.path.join(BASE, "dwd", "flow.csv")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    ordered = sorted(rows, key=lambda r: r["txn_id"])
    with open(dst, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["txn_id", "acct", "dc", "amount",
                    "txn_time", "post_time", "rev_of"])
        for r in ordered:
            w.writerow([r["txn_id"], r["acct"], r["dc"], r["amount"],
                        r["txn_time"], r["post_time"], r["rev_of"]])
    return len(ordered)


def build_dws(rows):
    # day is attributed by post_time (posting date).
    agg = defaultdict(lambda: [0, 0])  # (acct, day) -> [cnt, net_amt]
    for r in rows:
        day = r["post_time"][:10]
        cell = agg[(r["acct"], day)]
        cell[0] += 1
        cell[1] += r["amount"] if r["dc"] == "C" else -r["amount"]

    open_bal = {}
    with open(OPENING, "r", encoding="utf-8", newline="") as f:
        for rec in csv.DictReader(f):
            open_bal[rec["acct"]] = int(rec["open_bal"])

    dst = os.path.join(BASE, "dws", "acct_day.csv")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    n = 0
    with open(dst, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["acct", "day", "cnt", "net_amt", "eod_bal"])
        for acct, day in sorted(agg):
            cnt, net = agg[(acct, day)]
            bal = open_bal.get(acct, 0) + net
            open_bal[acct] = bal
            w.writerow([acct, day, cnt, net, bal])
            n += 1
    return n


def write_summary(dwd_rows, dws_rows, reversals, orphan_reversals):
    notes = ("day按post_time(记账时间)的日期归属;"
             "冲正按自身借贷方向计入cnt与net_amt;"
             "rev_of在月包中找不到的孤儿冲正共%d笔,仍保留并计入聚合" % orphan_reversals)
    with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
        f.write("dwd_rows %d\n" % dwd_rows)
        f.write("dws_rows %d\n" % dws_rows)
        f.write("reversals %d\n" % reversals)
        f.write("notes %s\n" % notes)


def main():
    state = load_state()
    if all(state.get(layer) == "done" for layer in LAYERS):
        return  # nothing to do; rerun is a no-op

    rows = None
    dwd_rows = dws_rows = reversals = orphan_reversals = None

    if state.get("ods") != "done":
        build_ods()
        state["ods"] = "done"
        save_state(state)
        log_done("ods")

    if state.get("dwd") != "done":
        if rows is None:
            rows = read_source_rows()
        dwd_rows = build_dwd(rows)  # half-written layer is rebuilt whole
        state["dwd"] = "done"
        save_state(state)
        log_done("dwd")

    if state.get("dws") != "done":
        if rows is None:
            rows = read_source_rows()
        dws_rows = build_dws(rows)
        reversals = sum(1 for r in rows if r["rev_of"])
        ids = {r["txn_id"] for r in rows}
        orphan_reversals = sum(1 for r in rows
                               if r["rev_of"] and r["rev_of"] not in ids)
        state["dws"] = "done"
        save_state(state)
        log_done("dws")

    if dwd_rows is None or dws_rows is None:
        # dws finished this run needs the summary; if only some layers were
        # already done, recompute the missing counts without rewriting data.
        if rows is None:
            rows = read_source_rows()
        if dwd_rows is None:
            dwd_rows = len(rows)
        if dws_rows is None:
            dws_rows = len({(r["acct"], r["post_time"][:10]) for r in rows})
        if reversals is None:
            reversals = sum(1 for r in rows if r["rev_of"])
            ids = {r["txn_id"] for r in rows}
            orphan_reversals = sum(1 for r in rows
                                   if r["rev_of"] and r["rev_of"] not in ids)
    if state.get("dws") == "done" and dws_rows is not None:
        write_summary(dwd_rows, dws_rows, reversals, orphan_reversals)


if __name__ == "__main__":
    main()
