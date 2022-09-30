import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import sys

output = sys.argv[1] if len(sys.argv) > 1 else ''
data = pd.read_csv(sys.stdin)

labels = sorted(set(data['label']))

indexers = list(set(data['indexer']))
indexers.sort(key=lambda i: data[(data['indexer'] == i) & (data['label'] == labels[0])]['label'].count())

matplotlib.rcParams['font.family'] = 'monospace'
fig, axes = plt.subplots(len(labels), 2, figsize=(12, 8), sharex='col', sharey=True, constrained_layout=True)

for (row, label) in enumerate(labels):
    d = data[data['label'] == label]
    print(d)
    axes[row, 0].barh(d['indexer'], d['selections'])
    axes[row, 0].invert_yaxis()

    axes[row, 1].barh(d['indexer'], d['fees'])
    axes[row, 1].invert_yaxis()

axes[-1, 0].set_xlabel('total selections')
axes[-1, 1].set_xlabel('total fees (GRT)')

if output == '':
    plt.show()
else:
    print('saving to', output)
    plt.savefig(output)
