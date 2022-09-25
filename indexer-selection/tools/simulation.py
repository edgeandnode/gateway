import matplotlib.pyplot as plt
import pandas as pd

data = pd.read_csv(f'test-outputs/simulation.csv')
data.sort_values('selections', inplace=True, ascending=False)
print(data)

fig, axes = plt.subplots(1, 2, figsize=(12, 8), sharey=True, constrained_layout=True)

axes[0].barh(data['indexer'], data['selections'])
axes[0].invert_yaxis()
axes[0].set_xlabel('total selections')

axes[1].barh(data['indexer'], data['fees'])
axes[1].invert_yaxis()
axes[1].set_xlabel('total fees (GRT)')

plt.savefig(f'test-outputs/simulation.svg')
