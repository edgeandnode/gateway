{
    api_key_payment_required: true,
    exchange_rate_provider: "1.0",
    // fisherman: "http://172.17.0.1:",
    gateway_instance_count: 1,
    graph_env_id: "localnet",
    indexer_selection_retry_limit: 2,
    ipfs: "http://172.17.0.1:5001/api/v0/cat?arg=",
    ip_rate_limit: 100,
    log_json: false,
    min_indexer_version: "0.0.0",
    network_subgraph: "http://172.17.0.1:8000/subgraphs/name/graph-network",
    port_api: 6700,
    port_metrics: 7301,
    query_budget_discount: 0.5,
    query_budget_scale: 1.5,
    # restricted_deployments: "=",
    signer_key: "myth like bonus scare over problem client lizard pioneer submit female collect",
    # special_api_keys:
    studio_auth: "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6WyIqIl0sImlhdCI6MTY4NjIzMTIyNiwiZXhwIjoxNzQ5MzQ2NDI2fQ.UmVeKAkmluD45-gMIUsjyQvHRIES0oUwemzFccr414sT24DnL1M8QrA7XG7pY5zltPeEOJvypPQJr7dpo9RFDg",
    studio_url: "http://172.17.0.1:4003/admin/v1",
    # subscriptions_subgraph:

    chains: [
        {
            name: "hardhat",
            rpc: "http://172.17.0.1:8545",
            poll_hz: 3,
            block_rate_hz: 0.5,
        },
    ],

    kafka: {
        "bootstrap.servers": "172.17.0.1:9092",
        // "security.protocol"
        // "sasl.mechanism"
        // "sasl.username"
        // "sasl.password"
        // "ssl.ca.location"
        // "ssl.key.location"
        // "ssl.certificate.location"
    },

    // subscriptions_contract: "",
    // subscriptions_chain_id: 1337,
    // subscriptions_owner: "0x90f8bf6a479f320ead074411a4b0e7944ea8c9c1",
    // subscriptions_subgraph: "http://172.17.0.1:8000/subgraphs/name/edgeandnode-subscriptions",
    // subscriptions_subgraph: "http://172.17.0.1:/api/deployments/id/",
    // subscriptions_ticket: "oWZzaWduZXJUkPi_akefMg6tB0QRpLDnlE6oycEtTrC6JCzrddbf1iRRG7tiwosxzOq-Oy1gnKNmeThRiHoVSWA_d6wXVEoXN8d6eHk6dtcUG6fLBpyTyLSE8F0IHA",
    // subscription_tiers: [
    //     { payment_rate: "1", queries_per_minute: 10 },
    //     { payment_rate: "10", queries_per_minute: 100 },
    // ],
}
