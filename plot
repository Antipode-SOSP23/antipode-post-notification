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
import inspect
import argparse
import yaml


#--------------
# PLOTS
#--------------
def plot__consistency_window(config):
  # Apply the default theme
  sns.set_theme(style='ticks')
  plt.rcParams["figure.figsize"] = [6,2.9]
  plt.rcParams["figure.dpi"] = 600
  plt.rcParams['axes.labelsize'] = 'small'

  # <Post Storage>-SNS
  data = {}
  for gather_path in config['gather_paths']:
    # 'Post Storage': 'DynamoDB',
    # # 'Overhead Visibility latency %': (1551.94 / 544.02) * 100.0,
    # # EU->US
    # 'Original': round(544.02),
    # 'Antipode': round(1551.94),
    traces_filepath = GATHER_PATH / gather_path / 'traces.csv'

    # ignoring notification_storage since we fix one for this plot
    post_storage , _ = traces_filepath.parts[-3].split('-')

    # check if combination already in the data folder
    if post_storage not in data:
      data[post_storage] = {
        'Post Storage': STORAGE_PRETTY_NAMES[post_storage],
      }

    # find out if antipode is enabled
    run_type = 'Antipode' if 'antipode' in traces_filepath.parts[-2] else 'Original'

    df = pd.read_csv(traces_filepath,sep=';',index_col=0)
    data[post_storage][run_type] = round(df['writer_visibility_latency_ms'].mean())

  data = list(data.values())

  # for each Original / Antipode pair we take the Original out of antipode so
  # stacked bars are presented correctly
  for d in data:
    d['Antipode'] = max(0, d['Antipode'] - d['Original'])


  df = pd.DataFrame.from_records(data).set_index('Post Storage')
  log = True
  if log:
    ax = df.plot(kind='bar', stacked=True, logy=True)
    ax.set_ylim(1, 100000)
    plt.xticks(rotation = 0)
  else:
    ax = df.plot(kind='bar', stacked=True, logy=False)
    plt.xticks(rotation = 0)

  ax.set_ylabel('Consistency Window (ms)')
  ax.set_xlabel('')

  # plot baseline bar
  ax.bar_label(ax.containers[0], label_type='center', fontsize=9, weight='bold', color='white')
  # plot overhead bar
  labels = [ f"+ {round(e)}" for e in ax.containers[1].datavalues ]
  ax.bar_label(ax.containers[1], labels=labels, label_type='edge', fontsize=9, weight='bold', color='black')

  # reverse order of legend
  handles, labels = ax.get_legend_handles_labels()
  ax.legend(handles[::-1], labels[::-1])

  # save with a unique timestamp
  plt.tight_layout()
  plot_filename = f"consistency_window__{datetime.now().strftime('%Y%m%d%H%M')}"
  plt.savefig(PLOTS_PATH / plot_filename, bbox_inches = 'tight', pad_inches = 0.1)
  print(f"[INFO] Saved plot '{plot_filename}'")


def plot__delay_vs_per_inconsistencies(config):
  from matplotlib.ticker import ScalarFormatter
  import matplotlib.ticker as mtick

  # Apply the default theme
  sns.set_theme(style='ticks')
  plt.figure(figsize=(6,2.75), dpi=600)
  plt.rcParams['axes.labelsize'] = 'small'

  # Using [0-9] pattern
  data = {}
  for c in config['combinations']:
    post_storage = c['post_storage']
    notification_storage = c['notification_storage']
    combination = f"{post_storage}-{notification_storage}"

    pattern= str(ROOT_PATH / 'gather' / combination / f"{config['writer_region']}-{config['reader_region']}__{config['num_requests']}__delay-*")
    for path in glob.glob(pattern):
      delay_ms = int(re.search(r'delay-([0-9]*)ms', path).group(1))

      # open traces.info file
      per_inconsistencies = None
      with open(Path(path) / 'traces.info', 'r') as f:
        for line in f:
          if line.startswith('[%_INCONSISTENCIES]'):
            per_inconsistencies = float(line.split(' ')[1])

      # write data entry
      if combination not in data:
        data[combination] = { }
      if delay_ms not in data[combination]:
        data[combination][delay_ms] = []
      data[combination][delay_ms].append(per_inconsistencies)

  # pick max for each entry
  data = [
    {
      'delay_ms': delay_ms,
      'per_inconsistencies': min(per_inconsistencies),
      # only consider post-storage
      'combination': STORAGE_PRETTY_NAMES[combination.split('-')[0]],
    } for combination, d in data.items() for delay_ms, per_inconsistencies in d.items()
  ]
  # build df
  df = pd.DataFrame.from_records(data).sort_values(by=['combination','delay_ms'])
  pp(df)

  ax = sns.lineplot(data=df, x="delay_ms", y="per_inconsistencies", hue="combination", linewidth=3)
  # set log scale
  ax.set_xscale('log')
  # change log scale ticks to scalar
  for axis in [ax.xaxis, ax.yaxis]:
    axis.set_major_formatter(ScalarFormatter())
  # change y scale to 0-100
  ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
  # change the axis labels
  ax.set_ylabel('% Inconsistencies')
  ax.set_xlabel('Artificial Delay (ms)')
  # legend outside
  ax.legend_.set_title(None)
  # ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
  # ax.legend(loc='lower center', bbox_to_anchor=(1, 0.5))
  sns.move_legend(ax, "lower center", bbox_to_anchor=(.5, 1), ncol=4, title=None, frameon=True)


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
GATHER_PATH = ROOT_PATH / 'gather'
PLOT_NAMES = [name.split('plot__')[1] for name,_ in inspect.getmembers(sys.modules[__name__]) if name.startswith('plot__')]
STORAGE_PRETTY_NAMES = {
  's3': 'S3',
  'sns': 'SNS',
  'mysql': 'MySQL',
  'cache': 'Redis',
  'dynamo': 'DynamoDB',
}

#--------------
# CLI
#--------------
if __name__ == '__main__':
  # parse arguments
  main_parser = argparse.ArgumentParser()
  main_parser.add_argument('config', type=argparse.FileType('r', encoding='UTF-8'), help="Plot config to load")
  main_parser.add_argument('--plots', nargs='*', choices=PLOT_NAMES, default=PLOT_NAMES, required=False, help="Plot only the passed plot names")

  # parse args
  args = vars(main_parser.parse_args())
  # load yaml
  args['config'] = (yaml.safe_load(args['config']) or {})

  for plot_name in set(args['config'].keys()) & set(args['plots']):
    gather_paths = args['config'][plot_name]
    getattr(sys.modules[__name__], f"plot__{plot_name}")(gather_paths)