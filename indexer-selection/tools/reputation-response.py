import matplotlib.pyplot as plt
import pandas as pd

def plot_reputation_response(name, key, unit):
    data = pd.read_csv(f'test-outputs/reputation-{name}-response.csv')
    print(data)

    fig, axes = plt.subplots(3, 2, sharex=True, sharey=True, figsize=(12, 8), constrained_layout=True)

    success_rates = sorted(set(data['success_rate']))
    values = sorted(set(data[key]))
    print('success_rates:', success_rates)
    print('values:', values)

    for i, v in enumerate(values):
        row, col = i // 2, i % 2
        p = axes[row, col]

        for success_rate in success_rates:
            d = data[(data[key] == v) & (data['success_rate'] == success_rate)]
            p.plot(d['t_m'], d['utility'], label=f'{success_rate}')

        p.set_ylim((0.0, 1.0))
        p.grid()
        p.set_title(f'{v}{unit} {name}')
        if col == 0: p.set_ylabel('utility')
        if row == 2: p.set_xlabel('t (minute)')
        if row == 2 and col == 1: p.legend(success_rates, title='Success Rate')

    fig.suptitle(f'Reputation {name.title()} Response', fontsize=16)
    plt.savefig(f'test-outputs/reputation-{name}-response.svg')

plot_reputation_response('outage', 'outage_duration_m', ' minute')
plot_reputation_response('penalty', 'penalty', '')
