#!/usr/bin/env python3
"""Plot static-vs-dynamic comparison from results/results_*.csv."""
import glob
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
df = pd.concat([pd.read_csv(p) for p in glob.glob(os.path.join(ROOT, "results", "results_*.csv"))])
df["success"] = df["success"].astype(str).str.lower().eq("true")
order = ["normal", "concurrent", "congestion", "linkfail"]
labels = ["static", "dynamic"]


def bar(metric, title, fname, only_ok=True):
    d = df[df.success] if only_ok else df
    g = d.groupby(["scenario", "label"])[metric].mean().unstack("label").reindex(order)
    ax = g[[l for l in labels if l in g]].plot.bar(rot=0, figsize=(7, 4))
    ax.set_title(title); ax.set_ylabel(metric); ax.set_xlabel("scenario")
    plt.tight_layout(); plt.savefig(os.path.join(ROOT, "results", fname), dpi=150); plt.close()


bar("throughput_MBps", "Average throughput (successful runs)", "throughput.png")
bar("duration_s", "Completion time", "duration.png")
bar("max_ack_gap_s", "Longest stall between ACKs (recovery time proxy)", "recovery.png")
rate = df.groupby(["scenario", "label"])["success"].mean().unstack("label").reindex(order) * 100
rate.plot.bar(rot=0, figsize=(7, 4), title="Backup success rate (%)")
plt.tight_layout(); plt.savefig(os.path.join(ROOT, "results", "success_rate.png"), dpi=150)
print(df.groupby(["scenario", "label"])[["throughput_MBps", "duration_s", "max_ack_gap_s", "success"]].mean())
