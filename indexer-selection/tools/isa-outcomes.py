# {cluster=~"mainnet-gateway-04-us-east", app="gateway"} |= "Indexer attempt" |= "QmXDs3UikxJBLwPztj3tLhyFuaV51tEftjSY1AHjBUz3CH"
# cargo run --bin sim -- input.txt | python tools/isa-outcomes.py

import numpy as np
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

    selections = d.groupby(['indexer'])['label'].count().reset_index().sort_values(by=['indexer'], key=lambda i: i.apply(indexers.index))
    axes[row, 0].barh(selections['indexer'], selections['label'])
    axes[row, 0].invert_yaxis()

    selections = d.groupby(['indexer'])['fee'].sum().reset_index().sort_values(by=['indexer'], key=lambda i: i.apply(indexers.index))
    axes[row, 1].barh(selections['indexer'], selections['fee'])
    axes[row, 1].invert_yaxis()

axes[-1, 0].set_xlabel('total selections')
axes[-1, 1].set_xlabel('total fees (GRT)')

if output == '':
    plt.show()
else:
    print('saving to', output)
    plt.savefig(output)
