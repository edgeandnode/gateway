use serde_json::json;
use thegraph_core::{allocation_id, deployment_id, indexer_id, subgraph_id};
use tracing_subscriber::{fmt::TestWriter, EnvFilter};

use super::{indexer_processing::IndexingRawInfo, pre_processing};
use crate::network::subgraph_client::types::Subgraph as SubgraphData;

/// Test method to initialize the tests tracing subscriber.
fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .compact()
        .with_writer(TestWriter::default())
        .try_init();
}

/// De-serialize the given JSON data as it was fetched from the network.
fn network_data(data: serde_json::Value) -> Vec<SubgraphData> {
    serde_json::from_value(data).expect("deserialization failed")
}

#[test]
fn indexers_data_pre_processing() {
    init_test_tracing();

    //* Given
    // Multiple subgraphs with multiple versions and indexers:
    // - Subgraph 1: 21dvLGCdpj4TNQXt7azhjc2sZhj2j5fWXuYCYG6z3mjP
    //   - Version 1: QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN
    //     - Indexer: 0xedca8740873152ff30a2696add66d1ab41882beb (2 allocations)
    //     - Indexer: 0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491
    //   - Version 0: QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh
    //     - Indexer: 0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491
    // - Subgraph 2: 223LR19dRLKChVVy8xH4bXvG9gjnFvmm73M6qDh8BFLf
    //   - Version 1: QmboQC3YgcxwqtmaV71bFxEvepbsq7fmSWgBARifcyJkj9
    //     - Indexer: 0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491
    //   - Version 0: QmWs3jKfZDhFp5Hrq4qhxsSo46DkCnJJsaUqn2MggKcNfz
    //     - Indexer: 0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491
    let data = network_data(json!([
      {
        "id": "21dvLGCdpj4TNQXt7azhjc2sZhj2j5fWXuYCYG6z3mjP",
        "versions": [
          {
            "version": 1,
            "subgraphDeployment": {
              "ipfsHash": "QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN",
              "manifest": {
                "network": "gnosis",
                "startBlock": "25313137"
              },
              "indexerAllocations": [
                {
                  "id": "0x8de241c35f8bc02ae9ad635e273372dd083f6520",
                  "allocatedTokens": "2000000000000000000",
                  "indexer": {
                    "id": "0xedca8740873152ff30a2696add66d1ab41882beb",
                    "stakedTokens": "1581895764461196487409847",
                    "url": "https://arbitrum.graph.pinax.network/"
                  }
                },
                {
                  "id": "0xf29f2d086abf0b92cf119575d000b45e331a4df7",
                  "allocatedTokens": "1000000000000000000",
                  "indexer": {
                    "id": "0xedca8740873152ff30a2696add66d1ab41882beb",
                    "stakedTokens": "1581895764461196487409847",
                    "url": "https://arbitrum.graph.pinax.network/"
                  }
                },
                {
                  "id": "0xcc3f326bdbfcb6fc730e04d859e6103f31cd691c",
                  "allocatedTokens": "0",
                  "indexer": {
                    "id": "0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491",
                    "stakedTokens": "100000000000000000000000",
                    "url": "https://indexer.upgrade.thegraph.com/"
                  }
                }
              ],
            },
          },
          {
            "version": 0,
            "subgraphDeployment": {
              "ipfsHash": "QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh",
              "manifest": {
                "network": "gnosis",
                "startBlock": "25313105"
              },
              "indexerAllocations": [
                {
                  "id": "0xa51c172268db23b0ec7bcf36b60d4cec374c1783",
                  "allocatedTokens": "0",
                  "indexer": {
                    "id": "0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491",
                    "stakedTokens": "100000000000000000000000",
                    "url": "https://indexer.upgrade.thegraph.com/"
                  }
                }
              ],
            },
          }
        ]
      },
      {
        "id": "223LR19dRLKChVVy8xH4bXvG9gjnFvmm73M6qDh8BFLf",
        "versions": [
          {
            "version": 1,
            "subgraphDeployment": {
              "ipfsHash": "QmboQC3YgcxwqtmaV71bFxEvepbsq7fmSWgBARifcyJkj9",
              "manifest": {
                "network": "arbitrum-sepolia",
                "startBlock": "24276088"
              },
              "indexerAllocations": [
                {
                  "id": "0x28220d396bf2c22717b07f4d767429b7d5b72b03",
                  "allocatedTokens": "0",
                  "indexer": {
                    "id": "0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491",
                    "stakedTokens": "100000000000000000000000",
                    "url": "https://indexer.upgrade.thegraph.com/"
                  }
                }
              ],
            },
          },
          {
            "version": 0,
            "subgraphDeployment": {
              "ipfsHash": "QmWs3jKfZDhFp5Hrq4qhxsSo46DkCnJJsaUqn2MggKcNfz",
              "manifest": {
                "network": "arbitrum-sepolia",
                "startBlock": "24276088"
              },
              "indexerAllocations": [
                {
                  "id": "0x070b3036035489055d59f93efb63b80c7031ebca",
                  "allocatedTokens": "0",
                  "indexer": {
                    "id": "0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491",
                    "stakedTokens": "100000000000000000000000",
                    "url": "https://indexer.upgrade.thegraph.com/"
                  }
                }
              ],
            },
          }
        ]
      }
    ]));

    //* When
    let info = pre_processing::into_internal_indexers_raw_info(data.iter());

    //* Then
    let indexer_1_address = indexer_id!("bdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491");
    let indexer_2_address = indexer_id!("edca8740873152ff30a2696add66d1ab41882beb");

    let deployment_id_1 = deployment_id!("QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN");
    let deployment_id_2 = deployment_id!("QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh");
    let deployment_id_3 = deployment_id!("QmboQC3YgcxwqtmaV71bFxEvepbsq7fmSWgBARifcyJkj9");
    let deployment_id_4 = deployment_id!("QmWs3jKfZDhFp5Hrq4qhxsSo46DkCnJJsaUqn2MggKcNfz");

    //- Assert indexers' info aggregation
    assert_eq!(info.len(), 2);
    assert!(info.contains_key(&indexer_1_address));
    assert!(info.contains_key(&indexer_2_address));

    let indexer_1 = info.get(&indexer_1_address).expect("indexer not found");
    let indexer_2 = info.get(&indexer_2_address).expect("indexer not found");

    //- Assert indexers' url validity
    // 0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491
    assert_eq!(indexer_1.url.scheme(), "https");
    assert!(indexer_1.url.has_host());
    // 0xedca8740873152ff30a2696add66d1ab41882beb
    assert_eq!(indexer_2.url.scheme(), "https");
    assert!(indexer_2.url.has_host());

    //- Assert indexer's staked tokens value
    // 0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491
    assert_eq!(indexer_1.staked_tokens, 100000000000000000000000);
    // 0xedca8740873152ff30a2696add66d1ab41882beb
    assert_eq!(indexer_2.staked_tokens, 1581895764461196487409847);

    //- Assert indexer's associated deployments

    // 0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491
    let indexer_1_expected_deployments = [
        deployment_id_1,
        deployment_id_2,
        deployment_id_3,
        deployment_id_4,
    ];
    assert_eq!(
        indexer_1.indexings.len(),
        indexer_1_expected_deployments.len()
    );
    assert!(indexer_1
        .indexings
        .keys()
        .all(|d| indexer_1_expected_deployments.contains(d)));

    // 0xedca8740873152ff30a2696add66d1ab41882beb
    let indexer_2_expected_deployments = [deployment_id_1];
    assert_eq!(
        indexer_2.indexings.len(),
        indexer_2_expected_deployments.len()
    );
    assert!(indexer_2
        .indexings
        .keys()
        .all(|d| indexer_2_expected_deployments.contains(d)));

    //- Assert indexers' indexings' largest allocation info
    // 0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491
    let indexer_1_indexing_1_info = indexer_1
        .indexings
        .get(&deployment_id_1)
        .expect("indexing info not found");
    let indexer_1_indexing_2_info = indexer_1
        .indexings
        .get(&deployment_id_2)
        .expect("indexing info not found");
    let indexer_1_indexing_3_info = indexer_1
        .indexings
        .get(&deployment_id_3)
        .expect("indexing info not found");
    let indexer_1_indexing_4_info = indexer_1
        .indexings
        .get(&deployment_id_4)
        .expect("indexing info not found");

    let expected_indexer_1_indexing_1_info = IndexingRawInfo {
        largest_allocation: allocation_id!("cc3f326bdbfcb6fc730e04d859e6103f31cd691c"),
        total_allocated_tokens: 0,
    };
    let expected_indexer_1_indexing_2_info = IndexingRawInfo {
        largest_allocation: allocation_id!("a51c172268db23b0ec7bcf36b60d4cec374c1783"),
        total_allocated_tokens: 0,
    };
    let expected_indexer_1_indexing_3_info = IndexingRawInfo {
        largest_allocation: allocation_id!("28220d396bf2c22717b07f4d767429b7d5b72b03"),
        total_allocated_tokens: 0,
    };
    let expected_indexer_1_indexing_4_info = IndexingRawInfo {
        largest_allocation: allocation_id!("070b3036035489055d59f93efb63b80c7031ebca"),
        total_allocated_tokens: 0,
    };

    assert_eq!(
        indexer_1_indexing_1_info,
        &expected_indexer_1_indexing_1_info
    );
    assert_eq!(
        indexer_1_indexing_2_info,
        &expected_indexer_1_indexing_2_info
    );
    assert_eq!(
        indexer_1_indexing_3_info,
        &expected_indexer_1_indexing_3_info
    );
    assert_eq!(
        indexer_1_indexing_4_info,
        &expected_indexer_1_indexing_4_info
    );

    // 0xedca8740873152ff30a2696add66d1ab41882beb
    let indexer_2_indexing_1_info = indexer_2
        .indexings
        .get(&deployment_id_1)
        .expect("indexing info not found");

    let expected_indexer_2_indexing_1_info = IndexingRawInfo {
        largest_allocation: allocation_id!("8de241c35f8bc02ae9ad635e273372dd083f6520"),
        total_allocated_tokens: 3000000000000000000,
    };

    assert_eq!(
        indexer_2_indexing_1_info,
        &expected_indexer_2_indexing_1_info
    );
}

#[test]
fn subgraphs_data_pre_processing() {
    init_test_tracing();

    //* Given
    // Multiple subgraphs with multiple versions and indexers:
    // - Subgraph 1: 21dvLGCdpj4TNQXt7azhjc2sZhj2j5fWXuYCYG6z3mjP
    //   - Version 1: QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN
    //   - Version 0: QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh
    // - Subgraph 2: 223LR19dRLKChVVy8xH4bXvG9gjnFvmm73M6qDh8BFLf
    //   - Version 1: QmboQC3YgcxwqtmaV71bFxEvepbsq7fmSWgBARifcyJkj9
    // - Subgraph 2: 223LR19dRLKChVVy8xH4bXvG9gjnFvmm73M6qDh8BFLf (duplicate -> ignored)
    //   - Version 0: QmWs3jKfZDhFp5Hrq4qhxsSo46DkCnJJsaUqn2MggKcNfz
    // - Subgraph 3: 2gWLd9Aw4VRCPQHcXrxBSVGWEdBu3VL8arCckxRUbeAA
    //   - Version 1: QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN (duplicate -> not-aggregated)
    let data = network_data(json!([
      {
        "id": "21dvLGCdpj4TNQXt7azhjc2sZhj2j5fWXuYCYG6z3mjP",
        "versions": [
          {
            "version": 1,
            "subgraphDeployment": {
              "ipfsHash": "QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN",
              "manifest": {
                "network": "gnosis",
                "startBlock": "25313137"
              },
              "indexerAllocations": [
                {
                  "id": "0xcc3f326bdbfcb6fc730e04d859e6103f31cd691c",
                  "allocatedTokens": "0",
                  "indexer": {
                    "id": "0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491",
                    "stakedTokens": "100000000000000000000000",
                    "url": "https://indexer.upgrade.thegraph.com/"
                  }
                }
              ],
            },
          },
          {
            "version": 0,
            "subgraphDeployment": {
              "ipfsHash": "QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh",
              "manifest": {
                "network": "gnosis",
                "startBlock": "25313105"
              },
              "indexerAllocations": [
                {
                  "id": "0xa51c172268db23b0ec7bcf36b60d4cec374c1783",
                  "allocatedTokens": "0",
                  "indexer": {
                    "id": "0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491",
                    "stakedTokens": "100000000000000000000000",
                    "url": "https://indexer.upgrade.thegraph.com/"
                  }
                }
              ],
            },
          }
        ]
      },
      {
        "id": "223LR19dRLKChVVy8xH4bXvG9gjnFvmm73M6qDh8BFLf",
        "versions": [
          {
            "version": 1,
            "subgraphDeployment": {
              "ipfsHash": "QmboQC3YgcxwqtmaV71bFxEvepbsq7fmSWgBARifcyJkj9",
              "manifest": {
                "network": "arbitrum-sepolia",
                "startBlock": "24276088"
              },
              "indexerAllocations": [
                {
                  "id": "0x28220d396bf2c22717b07f4d767429b7d5b72b03",
                  "allocatedTokens": "0",
                  "indexer": {
                    "id": "0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491",
                    "stakedTokens": "100000000000000000000000",
                    "url": "https://indexer.upgrade.thegraph.com/"
                  }
                }
              ],
            },
          },
        ]
      },
      {
        "id": "223LR19dRLKChVVy8xH4bXvG9gjnFvmm73M6qDh8BFLf",
        "versions": [
          {
            "version": 0,
            "subgraphDeployment": {
              "ipfsHash": "QmWs3jKfZDhFp5Hrq4qhxsSo46DkCnJJsaUqn2MggKcNfz",
              "manifest": {
                "network": "arbitrum-sepolia",
                "startBlock": "24276088"
              },
              "indexerAllocations": [
                {
                  "id": "0x070b3036035489055d59f93efb63b80c7031ebca",
                  "allocatedTokens": "0",
                  "indexer": {
                    "id": "0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491",
                    "stakedTokens": "100000000000000000000000",
                    "url": "https://indexer.upgrade.thegraph.com/"
                  }
                }
              ],
            },
          }
        ]
      },
      {
        "id": "2gWLd9Aw4VRCPQHcXrxBSVGWEdBu3VL8arCckxRUbeAA",
        "versions": [
          {
            "version": 1,
            "subgraphDeployment": {
              "ipfsHash": "QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN",
              "manifest": {
                "network": "gnosis-chiado",
                "startBlock": "25313137"
              },
              "indexerAllocations": [
                {
                  "id": "0x3afbf91a22d264d2d6fb46fa828ecc3dce687e72",
                  "allocatedTokens": "0",
                  "indexer": {
                    "id": "0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491",
                    "stakedTokens": "100000000000000000000000",
                    "url": "https://indexer.upgrade.thegraph.com/"
                  }
                }
              ],
            },
          }
        ]
      }
    ]));

    //* When
    let info = pre_processing::into_internal_subgraphs_raw_info(data.into_iter());

    //* Then
    let subgraph_1 = subgraph_id!("21dvLGCdpj4TNQXt7azhjc2sZhj2j5fWXuYCYG6z3mjP");
    let subgraph_2 = subgraph_id!("223LR19dRLKChVVy8xH4bXvG9gjnFvmm73M6qDh8BFLf");
    let subgraph_3 = subgraph_id!("2gWLd9Aw4VRCPQHcXrxBSVGWEdBu3VL8arCckxRUbeAA");

    let deployment_id_1 = deployment_id!("QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN");
    let deployment_id_2 = deployment_id!("QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh");
    let deployment_id_3 = deployment_id!("QmboQC3YgcxwqtmaV71bFxEvepbsq7fmSWgBARifcyJkj9");
    let deployment_id_4 = deployment_id!("QmWs3jKfZDhFp5Hrq4qhxsSo46DkCnJJsaUqn2MggKcNfz");

    //- Assert subgraphs' info deduplication: The first subgraph occurrence prevails.
    assert_eq!(info.len(), 3);
    assert!(info.contains_key(&subgraph_1));
    assert!(info.contains_key(&subgraph_2));
    assert!(info.contains_key(&subgraph_3));

    let subgraph_1_info = info.get(&subgraph_1).expect("subgraph not found");
    let subgraph_2_info = info.get(&subgraph_2).expect("subgraph not found");
    let subgraph_3_info = info.get(&subgraph_3).expect("subgraph not found");

    //- Assert subgraphs' versions are ordered in descending order
    assert_eq!(subgraph_1_info.versions.len(), 2);
    assert!(subgraph_1_info
        .versions
        .windows(2)
        .all(|w| w[0].version > w[1].version));

    assert_eq!(subgraph_2_info.versions.len(), 1);
    assert_eq!(subgraph_3_info.versions.len(), 1);

    // 21dvLGCdpj4TNQXt7azhjc2sZhj2j5fWXuYCYG6z3mjP
    let subgraph_1_expected_deployments = [deployment_id_1, deployment_id_2];
    assert!(subgraph_1_info
        .versions
        .iter()
        .map(|v| &v.deployment.id)
        .all(|id| subgraph_1_expected_deployments.contains(id)));

    // 223LR19dRLKChVVy8xH4bXvG9gjnFvmm73M6qDh8BFLf
    let subgraph_2_expected_deployments = [deployment_id_3, deployment_id_4];
    assert!(subgraph_2_info
        .versions
        .iter()
        .map(|v| &v.deployment.id)
        .all(|id| subgraph_2_expected_deployments.contains(id)));

    // 2gWLd9Aw4VRCPQHcXrxBSVGWEdBu3VL8arCckxRUbeAA
    let subgraph_3_expected_deployments = [deployment_id_1];
    assert!(subgraph_3_info
        .versions
        .iter()
        .map(|v| &v.deployment.id)
        .all(|id| subgraph_3_expected_deployments.contains(id)));

    //- Assert subgraphs' deployments' association with the parent subgraph
    // 21dvLGCdpj4TNQXt7azhjc2sZhj2j5fWXuYCYG6z3mjP
    assert!(
        subgraph_1_info
            .versions
            .iter()
            .all(|v| v.deployment.subgraphs.len() == 1
                && v.deployment.subgraphs.contains(&subgraph_1))
    );

    // 223LR19dRLKChVVy8xH4bXvG9gjnFvmm73M6qDh8BFLf
    assert!(
        subgraph_2_info
            .versions
            .iter()
            .all(|v| v.deployment.subgraphs.len() == 1
                && v.deployment.subgraphs.contains(&subgraph_2))
    );

    // 2gWLd9Aw4VRCPQHcXrxBSVGWEdBu3VL8arCckxRUbeAA
    assert!(
        subgraph_3_info
            .versions
            .iter()
            .all(|v| v.deployment.subgraphs.len() == 1
                && v.deployment.subgraphs.contains(&subgraph_3))
    );
}

#[test]
fn deployments_data_pre_processing() {
    init_test_tracing();

    //* Given
    // Multiple subgraphs with multiple versions and indexers:
    // - Subgraph 1: 21dvLGCdpj4TNQXt7azhjc2sZhj2j5fWXuYCYG6z3mjP
    //   - Version 1: QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN
    //   - Version 0: QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh
    // - Subgraph 2: 223LR19dRLKChVVy8xH4bXvG9gjnFvmm73M6qDh8BFLf
    //   - Version 1: QmboQC3YgcxwqtmaV71bFxEvepbsq7fmSWgBARifcyJkj9
    // - Subgraph 2: 223LR19dRLKChVVy8xH4bXvG9gjnFvmm73M6qDh8BFLf    (duplicated subgraph -> ignored)
    //   - Version 0: QmWs3jKfZDhFp5Hrq4qhxsSo46DkCnJJsaUqn2MggKcNfz (duplicated subgraph -> alloc. not aggregated)
    // - Subgraph 3: 2gWLd9Aw4VRCPQHcXrxBSVGWEdBu3VL8arCckxRUbeAA
    //   - Version 1: QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN (duplicated deployment -> alloc. aggregated)
    let data = network_data(json!([
      {
        "id": "21dvLGCdpj4TNQXt7azhjc2sZhj2j5fWXuYCYG6z3mjP",
        "versions": [
          {
            "version": 1,
            "subgraphDeployment": {
              "ipfsHash": "QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN",
              "manifest": {
                "network": "gnosis",
                "startBlock": "25313137"
              },
              "indexerAllocations": [
                {
                  "id": "0xcc3f326bdbfcb6fc730e04d859e6103f31cd691c",
                  "allocatedTokens": "0",
                  "indexer": {
                    "id": "0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491",
                    "stakedTokens": "100000000000000000000000",
                    "url": "https://indexer.upgrade.thegraph.com/"
                  }
                }
              ],
            },
          },
          {
            "version": 0,
            "subgraphDeployment": {
              "ipfsHash": "QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh",
              "manifest": {
                "network": "gnosis",
                "startBlock": "25313105"
              },
              "indexerAllocations": [
                {
                  "id": "0xa51c172268db23b0ec7bcf36b60d4cec374c1783",
                  "allocatedTokens": "0",
                  "indexer": {
                    "id": "0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491",
                    "stakedTokens": "100000000000000000000000",
                    "url": "https://indexer.upgrade.thegraph.com/"
                  }
                }
              ],
            },
          }
        ]
      },
      {
        "id": "223LR19dRLKChVVy8xH4bXvG9gjnFvmm73M6qDh8BFLf",
        "versions": [
          {
            "version": 1,
            "subgraphDeployment": {
              "ipfsHash": "QmboQC3YgcxwqtmaV71bFxEvepbsq7fmSWgBARifcyJkj9",
              "manifest": {
                "network": "arbitrum-sepolia",
                "startBlock": "24276088"
              },
              "indexerAllocations": [
                {
                  "id": "0x28220d396bf2c22717b07f4d767429b7d5b72b03",
                  "allocatedTokens": "0",
                  "indexer": {
                    "id": "0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491",
                    "stakedTokens": "100000000000000000000000",
                    "url": "https://indexer.upgrade.thegraph.com/"
                  }
                }
              ],
            },
          },
        ]
      },
      {
        "id": "223LR19dRLKChVVy8xH4bXvG9gjnFvmm73M6qDh8BFLf",
        "versions": [
          {
            "version": 0,
            "subgraphDeployment": {
              "ipfsHash": "QmWs3jKfZDhFp5Hrq4qhxsSo46DkCnJJsaUqn2MggKcNfz",
              "manifest": {
                "network": "arbitrum-sepolia",
                "startBlock": "24276088"
              },
              "indexerAllocations": [
                {
                  "id": "0x070b3036035489055d59f93efb63b80c7031ebca",
                  "allocatedTokens": "0",
                  "indexer": {
                    "id": "0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491",
                    "stakedTokens": "100000000000000000000000",
                    "url": "https://indexer.upgrade.thegraph.com/"
                  }
                }
              ],
            },
          }
        ]
      },
      {
        "id": "2gWLd9Aw4VRCPQHcXrxBSVGWEdBu3VL8arCckxRUbeAA",
        "versions": [
          {
            "version": 1,
            "subgraphDeployment": {
              "ipfsHash": "QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN",
              "manifest": {
                "network": "gnosis-chiado",
                "startBlock": "25313137"
              },
              "indexerAllocations": [
                {
                  "id": "0x3afbf91a22d264d2d6fb46fa828ecc3dce687e72",
                  "allocatedTokens": "0",
                  "indexer": {
                    "id": "0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491",
                    "stakedTokens": "100000000000000000000000",
                    "url": "https://indexer.upgrade.thegraph.com/"
                  }
                }
              ],
            },
          }
        ]
      }
    ]));

    let subgraphs_info = pre_processing::into_internal_subgraphs_raw_info(data.into_iter());

    //* When
    let info = pre_processing::into_internal_deployments_raw_info(subgraphs_info.values());

    //* Then
    let subgraph_1 = subgraph_id!("21dvLGCdpj4TNQXt7azhjc2sZhj2j5fWXuYCYG6z3mjP");
    let subgraph_2 = subgraph_id!("223LR19dRLKChVVy8xH4bXvG9gjnFvmm73M6qDh8BFLf");
    let subgraph_3 = subgraph_id!("2gWLd9Aw4VRCPQHcXrxBSVGWEdBu3VL8arCckxRUbeAA");

    let deployment_id_1 = deployment_id!("QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN");
    let deployment_id_2 = deployment_id!("QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh");
    let deployment_id_3 = deployment_id!("QmboQC3YgcxwqtmaV71bFxEvepbsq7fmSWgBARifcyJkj9");

    //- Assert deployments' info aggregation
    assert_eq!(info.len(), 3);
    assert!(info.contains_key(&deployment_id_1));
    assert!(info.contains_key(&deployment_id_2));
    assert!(info.contains_key(&deployment_id_3));

    let deployment_1_info = info.get(&deployment_id_1).expect("deployment not found");
    let deployment_2_info = info.get(&deployment_id_2).expect("deployment not found");
    let deployment_3_info = info.get(&deployment_id_3).expect("deployment not found");

    //- Assert deployments have at least one associated subgraph
    // QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN
    let deployment_1_expected_subgraphs = [subgraph_1, subgraph_3];
    assert_eq!(deployment_1_info.subgraphs.len(), 2);
    assert!(deployment_1_info
        .subgraphs
        .iter()
        .all(|s| deployment_1_expected_subgraphs.contains(s)));

    // QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh
    let deployment_2_expected_subgraphs = [subgraph_1];
    assert_eq!(deployment_2_info.subgraphs.len(), 1);
    assert!(deployment_2_info
        .subgraphs
        .iter()
        .all(|s| deployment_2_expected_subgraphs.contains(s)));

    // QmboQC3YgcxwqtmaV71bFxEvepbsq7fmSWgBARifcyJkj9
    let deployment_3_expected_subgraphs = [subgraph_2];
    assert_eq!(deployment_3_info.subgraphs.len(), 1);
    assert!(deployment_3_info
        .subgraphs
        .iter()
        .all(|s| deployment_3_expected_subgraphs.contains(s)));

    //- Assert deployments' allocations are aggregated
    // QmaiXMTFDFPRKoXQceXwzuFYhAYDkUXHLmBVxLUQs4ZKsN
    let deployment_1_expected_allocations = [
        allocation_id!("cc3f326bdbfcb6fc730e04d859e6103f31cd691c"),
        allocation_id!("3afbf91a22d264d2d6fb46fa828ecc3dce687e72"),
    ];
    assert_eq!(
        deployment_1_info.allocations.len(),
        deployment_1_expected_allocations.len()
    );
    assert!(deployment_1_info
        .allocations
        .iter()
        .all(|a| deployment_1_expected_allocations.contains(&a.id)));

    // QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh
    let expected_deployment_2_allocations =
        [allocation_id!("a51c172268db23b0ec7bcf36b60d4cec374c1783")];
    assert_eq!(
        deployment_2_info.allocations.len(),
        expected_deployment_2_allocations.len()
    );
    assert!(deployment_2_info
        .allocations
        .iter()
        .all(|a| expected_deployment_2_allocations.contains(&a.id)));

    // QmboQC3YgcxwqtmaV71bFxEvepbsq7fmSWgBARifcyJkj9
    let expected_deployment_3_allocations =
        [allocation_id!("28220d396bf2c22717b07f4d767429b7d5b72b03")];
    assert_eq!(
        deployment_3_info.allocations.len(),
        expected_deployment_3_allocations.len()
    );
    assert!(deployment_3_info
        .allocations
        .iter()
        .all(|a| expected_deployment_3_allocations.contains(&a.id)));
}
