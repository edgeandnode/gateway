import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import sys

output = sys.argv[1] if len(sys.argv) > 1 else ''
data = pd.read_csv(sys.stdin)

labels = sorted(set(data.label))

data['sort_order'] = data.apply(
    lambda r: data[(data.indexer == r.indexer) & (data.label == labels[0])].selections.sum(),
    axis=1,
)
data = data.sort_values(by='sort_order')

matplotlib.rcParams['font.family'] = 'monospace'
fig, axes = plt.subplots(len(labels), 2, figsize=(16, 12), sharex='col', sharey=True, constrained_layout=True)

for (row, label) in enumerate(labels):
    d = data[data.label == label]
    print(d)
    axes[row, 0].barh(d.indexer, d.selections)
    axes[row, 0].invert_yaxis()

    axes[row, 1].barh(d.indexer, d.fees)
    axes[row, 1].invert_yaxis()

    axes[row, 0].set_yticks(range(0, d.shape[0]))
    axes[row, 0].set_yticklabels(d.apply(lambda r: f'{r.detail}\n({r.indexer})', axis=1))
    axes[row, 0].set_title(label)

axes[-1, 0].set_xlabel('total selections')
axes[-1, 1].set_xlabel('total fees (GRT)')

if output == '':
    plt.show()
else:
    print('saving to', output)
    plt.savefig(output)
