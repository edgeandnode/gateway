# {cluster=~"mainnet-gateway-04-us-east", app="gateway"} |= "Indexer attempt" |= "QmXDs3UikxJBLwPztj3tLhyFuaV51tEftjSY1AHjBUz3CH"
# cargo run --bin sim -- input.txt | python tools/isa-outcomes.py

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys

window = '30min'

data = pd.read_csv(sys.stdin)
data['indexer'] = data[['indexer', 'url']].agg(lambda x: f"{x[0]} ({x[1]})", axis = 1)

colors = px.colors.qualitative.D3
colors = {indexer: colors[i % len(colors)] for (i, indexer) in enumerate(data['indexer'].unique())}

labeled = {label: data[data['label'] == label].copy() for label in data['label'].unique()}
fig = make_subplots(
    rows = len(labeled),
    cols = 1,
    subplot_titles = list(labeled.keys()),
    shared_xaxes = True,
    vertical_spacing = 0.05,
)

for (i, (label, data)) in enumerate(labeled.items()):
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    data.index = data['timestamp']
    data['count'] = data['timestamp'].apply(lambda _: 1)
    data.drop(columns = ['timestamp', 'label', 'url'], inplace = True)

    indexers = list(data.groupby('indexer')['count'].sum().sort_values().index)
    for indexer in list(indexers):
        indexer_data = data[data['indexer'] == indexer]
        indexer_data = indexer_data['count'].resample(window).sum()
        total = data['count'].resample(window).sum()
        fig.add_trace(
            go.Scatter(
                x = indexer_data.index,
                y = indexer_data / total,
                stackgroup = 'one',
                name = indexer,
                legendgroup = indexer,
                showlegend = i == 0,
                line = dict(color = colors[indexer]),
            ),
            row = i + 1,
            col = 1,
        )

fig.show()
