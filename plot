#!/usr/bin/env python3
import os
import sys
import pandas as pd
from pprint import pprint as pp
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
import glob
import re

def visibility_latency_overhead__plot():
  # Apply the default theme
  sns.set_theme(style='ticks')
  plt.figure(figsize=(4,3))

  # <Post Storage>-SNS
  data = [
    #
    # REVERSE ORDER OF THE PLOT
    #
    {
      'Post Storage': 'Redis',
      # 'Overhead Visibility latency %': (864.08 / 375.04) * 100.0,
      # EU->US
      'Baseline': round(375.04),
      'Antipode': round(864.08),
      # EU->SG
      # 'Baseline': round(499.49),
      # 'Antipode': round(909.48),
    },
    {
      'Post Storage': 'DynamoDB',
      # 'Overhead Visibility latency %': (1551.94 / 544.02) * 100.0,
      # EU->US
      'Baseline': round(544.02),
      'Antipode': round(1551.94),
      # EU->SG
      # 'Baseline': round(679.25),
      # 'Antipode': round(2058.26),
    },
    {
      'Post Storage': 'MySQL',
      # antipode vl (MEAN) / baseline vl (MEAN)
      # 'Overhead Visibility latency %': (1173.65 / 1147.13) * 100.0,
      # EU->US
      'Baseline': round(1147.13),
      'Antipode': round(1173.65),
      # EU->SG
      # 'Baseline': round(1219.94),
      # 'Antipode': round(1662.87),
    },
    {
      'Post Storage': 'S3',
      # 'Overhead Visibility latency %': (18455.11 / 696.04) * 100.0,
      # EU->US
      'Baseline': round(696.04),
      'Antipode': round(18455.11),
      # EU->SG
      # 'Baseline': round(846.36),
      # 'Antipode': round(18532.72),
    },
  ]

  # for each Baseline / Antipode pair we take the Baseline out of antipode so
  # stacked bars are presented correctly
  for d in data:
    d['Antipode'] = max(0, d['Antipode'] - d['Baseline'])


  df = pd.DataFrame.from_records(data).set_index('Post Storage')
  log = True
  if log:
    ax = df.plot(kind='bar', stacked=True, logy=True)
    ax.set_ylim(1, 100000)
    plt.xticks(rotation = 0)
  else:
    ax = df.plot(kind='bar', stacked=True, logy=False)
    plt.xticks(rotation = 0)

  ax.set_ylabel('Visibility Latency (ms)')

  # plot baseline bar
  ax.bar_label(ax.containers[0], label_type='center', fontsize=9, weight='bold', color='white')
  # plot overhead bar
  labels = [ f"+ {round(e)}" for e in ax.containers[1].datavalues ]
  ax.bar_label(ax.containers[1], labels=labels, label_type='edge', fontsize=9, weight='bold', color='black')

  # save with a unique timestamp
  plt.tight_layout()
  plot_filename = f"visibility_latency_overhead__{datetime.now().strftime('%Y%m%d%H%M')}"
  plt.savefig(PLOTS_PATH / plot_filename, bbox_inches = 'tight', pad_inches = 0.1)
  print(f"[INFO] Saved plot '{plot_filename}'")


def delay_vs_per_inconsistencies__plot():
  # Apply the default theme
  sns.set_theme(style='ticks')
  plt.figure(figsize=(4,3))

  post_storage = 's3'
  notification_storage = 'sns'
  writer_region = 'eu'
  reader_region = 'us'
  num_requests = 1000

  # Using [0-9] pattern
  data = {}
  pattern= str(ROOT_PATH / 'gather' / f"{post_storage}-{notification_storage}" / f"{writer_region}-{reader_region}__{num_requests}__delay-*")
  for path in glob.glob(pattern):
    delay_ms = int(re.search(r'delay-([0-9]*)ms', path).group(1))

    # open traces.info file
    per_inconsistencies = None
    with open(Path(path) / 'traces.info', 'r') as f:
      for line in f:
        if line.startswith('[%_INCONSISTENCIES]'):
          per_inconsistencies = float(line.split(' ')[1])

    # write data entry
    if delay_ms not in data:
      data[delay_ms] = []
    data[delay_ms].append(per_inconsistencies)

  # pick max for each entry
  pp(data)
  data = [ { 'delay_ms': delay_ms, 'per_inconsistencies': max(per_inconsistencies) } for delay_ms, per_inconsistencies in data.items() ]

  # build df
  df = pd.DataFrame.from_records(data).set_index('delay_ms').sort_index(ascending=True)
  ax = df.plot(kind='line', logx=False)
  ax.set_ylabel('% Inconsistencies')
  ax.set_xlabel('Delay (ms)')
  # ax.set_xticks([ d['delay_ms'] for d in data])
  # plt.xticks(rotation = 90)

  # save with a unique timestamp
  plt.tight_layout()
  plot_filename = f"delay_vs_per_inconsistencies__{datetime.now().strftime('%Y%m%d%H%M')}"
  plt.savefig(PLOTS_PATH / plot_filename, bbox_inches = 'tight', pad_inches = 0.1)
  print(f"[INFO] Saved plot '{plot_filename}'")


#--------------
# CONSTANTS
#--------------
ROOT_PATH = Path(os.path.abspath(os.path.dirname(sys.argv[0])))
PLOTS_PATH = ROOT_PATH / 'plots'

# visibility_latency_overhead__plot()
delay_vs_per_inconsistencies__plot()