{
    api_key_payment_required: true,
    attestations: {
        chain_id: "1337",
        dispute_manager: "0x86072CbFF48dA3C1F01824a6761A03F105BCC697",
    },
    block_oracle_subgraph: "http://172.17.0.1:8000/subgraphs/name/block-oracle",
    chains: [
        {
            names: ["hardhat"],
            rpc: "http://172.17.0.1:8545",
        },
    ],
    exchange_rate_provider: 1.0,
    graph_env_id: "localnet",
    indexer_selection_retry_limit: 2,
    ipfs: "http://172.17.0.1:5001/api/v0/cat?arg=",
    ip_rate_limit: 100,
    kafka: {
        "bootstrap.servers": "172.17.0.1:9092",
    },
    log_json: false,
    min_graph_node_version: "0.33.0",
    min_indexer_version: "0.0.0",
    network_subgraph: "http://172.17.0.1:8000/subgraphs/name/graph-network",
    port_api: 6700,
    port_metrics: 7301,
    query_fees_target: 20e-6,
    scalar: {
        chain_id: "1337",
        signer: "0x6cbed15c793ce57650b9877cf6fa156fbef513c4e6134f022a85b1ffdd59b2a1",
        verifier: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    },
    studio_auth: "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6WyIqIl0sImlhdCI6MTcwODAwOTMzOCwiZXhwIjoxNzcxMTI0NTM4fQ.a_J9DgVlG7HKRw-KDPGGqu9jJ7hdHwuC3PpV7-UC-ueJ5SNfBUucBpbtPL1axwJYzfABZiNMkli12ZHO-0FZ3Q",
    studio_url: "http://172.17.0.1:4003/admin/v1",
    subscriptions: {
        domains: [
            // {
            //     chain_id: 42161,
            //     contract: "0x482f58d3513E386036670404b35cB3F2DF67a750",
            // },
            // {
            //     chain_id: 421614,
            //     contract: "0x7F4965F6CcF06Ca656B58b9c1CF424D6eD2E4064",
            // }
        ],
        kafka_topic: "gateway_subscription_query_results",
        rate_per_query: 30,
        special_signers: [
            "0x4528fd7868c91ef64b9907450ee8d82dc639612c",
            "0x1cad9dd6d19eabc02898eb650407961bd5510139"
        ],
        subgraph: "https://api.thegraph.com/subgraphs/name/graphprotocol/subscriptions-arbitrum-sepolia",
    },
}
