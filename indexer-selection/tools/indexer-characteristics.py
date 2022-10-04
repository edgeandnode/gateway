# Converts "Indexer attempt" logs for some deployment into indexer characteristics

import datetime
import json
import pandas as pd
import requests
import sys

network_subgraph = 'https://gateway.thegraph.com/network'

def parse_log(log):
    timestamp = datetime.datetime.fromisoformat(log['timestamp'].replace('Z', '+00:00')).timestamp()
    fields = log['fields']
    return (
        int(timestamp * 1000),
        fields['indexer'],
        float(fields['fee']),
        float(fields['utility']),
        int(fields['blocks_behind']),
        float(fields['response_time_ms']),
        fields['status'] == '200',
    )

log_lines = [
    json.loads(l[l.index('{'):])
    for l in sys.stdin
    if ("timestamp" in l) and ("Indexer attempt" in l)
]
entries = [parse_log(l) for l in log_lines]

deployment = log_lines[0]['fields']['deployment']
print('deployment:', deployment, file=sys.stderr)

columns = ['t_ms', 'indexer', 'fee', 'utility', 'blocks_behind', 'response_time_ms', 'success']
data = pd.DataFrame(entries, columns=columns)

print('indexer,fee,blocks_behind,latency_ms,success_rate,allocation,stake')
for indexer in set(data['indexer']):
    d = data[data['indexer'] == indexer]
    fee = d['fee'].mean()
    blocks_behind = d['blocks_behind'].mean()
    latency_ms = d['response_time_ms'].mean()
    success_rate = d[d['success']].shape[0] / d.shape[0]

    response = requests.post(
        network_subgraph,
        json={'query': f'''{{
            allocations(where:{{
                activeForIndexer:"{indexer}"
                subgraphDeployment_:{{ipfsHash:"{deployment}"}}
            }}){{
                allocatedTokens
            }}
            indexer(id:"{indexer}"){{ stakedTokens }}
        }}'''},
    ).json()
    allocation = sum([
        float(a['allocatedTokens'][:-18])
        for a in response['data']['allocations']]
    )
    stake = float(response['data']['indexer']['stakedTokens'][:-18])

    print(f'{indexer},{fee:.20f},{blocks_behind},{latency_ms},{success_rate},{allocation:.0f},{stake:.0f}')
