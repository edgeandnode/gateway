use std::collections::{HashMap, HashSet};

use assert_matches::assert_matches;
use thegraph_core::{allocation_id, deployment_id, indexer_id, subgraph_id};
use tracing_subscriber::{fmt::TestWriter, EnvFilter};

use super::subgraph_processing::{
    self, AllocationInfo, DeploymentRawInfo, SubgraphRawInfo, SubgraphVersionRawInfo,
};
use crate::network::errors::{DeploymentError, SubgraphError};

// Test method to initialize the tests tracing subscriber.
fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .compact()
        .with_writer(TestWriter::default())
        .try_init();
}

#[test]
fn process_deployment_info_successfully() {
    init_test_tracing();

    //* Given
    // Graph Network Subgraph info (on 2024-05-27)
    let subgraph_id = subgraph_id!("DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp");
    let deployment_v100 = deployment_id!("QmZ5EcVesbdDidvgdMtd4h5xugVkEQWBgJ84CEouZrHGEq");
    let deployment_v110 = deployment_id!("QmZtNN8NbxjJ1KD5uKBYa7Gj29CT8xypSXnAmXbrLNTQgX");
    let raw_info = HashMap::from([
        (
            deployment_v110,
            DeploymentRawInfo {
                id: deployment_v110,
                allocations: vec![
                    AllocationInfo {
                        id: allocation_id!("177b557b12f22bb17a9d73dcc994d978dd6f5f89"),
                        indexer: indexer_id!("4e5c87772c29381bcabc58c3f182b6633b5a274a"),
                    },
                    AllocationInfo {
                        id: allocation_id!("2e9e707f8dfea2f03ef194c1b6478845377e6246"),
                        indexer: indexer_id!("bdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491"),
                    },
                    AllocationInfo {
                        id: allocation_id!("3c4a845623182c6cffe0da2c8f6d9e9128f34208"),
                        indexer: indexer_id!("269ebeee083ce6f70486a67dc8036a889bf322a9"),
                    },
                ],
                manifest_network: "arbitrum-one".to_string(),
                manifest_start_block: 42440000,
                subgraphs: HashSet::from([subgraph_id]),
                transferred_to_l2: false,
            },
        ),
        (
            deployment_v100,
            DeploymentRawInfo {
                id: deployment_v100,
                allocations: vec![AllocationInfo {
                    id: allocation_id!("89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2"),
                    indexer: indexer_id!("bdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491"),
                }],
                manifest_network: "arbitrum-one".to_string(),
                manifest_start_block: 42440000,
                subgraphs: HashSet::from([subgraph_id]),
                transferred_to_l2: false,
            },
        ),
    ]);

    //* When
    let info = subgraph_processing::process_deployments_info(raw_info);

    //* Then
    // Assert deployments processed info
    assert_eq!(info.len(), 2);
    assert!(info.contains_key(&deployment_v110));
    assert!(info.contains_key(&deployment_v100));

    let deployment_1 = info
        .get(&deployment_v110)
        .expect("deployment info not found")
        .as_ref()
        .expect("deployment blocked");

    assert_eq!(deployment_1.manifest_network, "arbitrum-one");
    assert_eq!(deployment_1.manifest_start_block, 42440000);

    assert_eq!(deployment_1.subgraphs.len(), 1);
    assert!(deployment_1.subgraphs.contains(&subgraph_id));

    assert!(!deployment_1.allocations.is_empty());

    let deployment_2 = info
        .get(&deployment_v100)
        .expect("deployment info not found")
        .as_ref()
        .expect("deployment blocked");

    assert_eq!(deployment_2.manifest_network, "arbitrum-one");
    assert_eq!(deployment_2.manifest_start_block, 42440000);

    assert_eq!(deployment_2.subgraphs.len(), 1);
    assert!(deployment_2.subgraphs.contains(&subgraph_id));

    assert!(!deployment_2.allocations.is_empty());
}

#[test]
fn block_deployment_when_transferred_to_l2() {
    init_test_tracing();

    //* Given
    // Graph Network Subgraph info (on 2024-05-27)
    let deployment_v001 = deployment_id!("QmU318BETTzmjUhBMDndQEaGqyP4rCSbiSZBZapaqNQQfF");
    let raw_info = HashMap::from([(
        deployment_v001,
        DeploymentRawInfo {
            id: deployment_v001,
            manifest_network: "mainnet".to_string(),
            manifest_start_block: 15685263,
            subgraphs: Default::default(),
            transferred_to_l2: true, // Marked as transferred
            allocations: vec![],     // No allocations
        },
    )]);

    //* When
    let info = subgraph_processing::process_deployments_info(raw_info);

    //* Then
    // Assert deployments processed info
    assert_eq!(info.len(), 1);
    assert!(info.contains_key(&deployment_v001));

    let deployment_err = info
        .get(&deployment_v001)
        .expect("deployment info not found")
        .as_ref()
        .expect_err("deployment not blocked");

    assert_matches!(deployment_err, &DeploymentError::TransferredToL2);
}

#[test]
fn process_subgraph_info_successfully() {
    init_test_tracing();

    //* Given
    // Graph Network Subgraph info (on 2024-05-27)
    let subgraph_id = subgraph_id!("DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp");
    let deployment_v100 = deployment_id!("QmZ5EcVesbdDidvgdMtd4h5xugVkEQWBgJ84CEouZrHGEq");
    let deployment_v110 = deployment_id!("QmZtNN8NbxjJ1KD5uKBYa7Gj29CT8xypSXnAmXbrLNTQgX");
    let raw_info = HashMap::from([(
        subgraph_id,
        SubgraphRawInfo {
            id: subgraph_id,
            id_on_l2: None,
            versions: vec![
                SubgraphVersionRawInfo {
                    version: 1,
                    deployment: DeploymentRawInfo {
                        id: deployment_v110,
                        allocations: vec![
                            AllocationInfo {
                                id: allocation_id!("177b557b12f22bb17a9d73dcc994d978dd6f5f89"),
                                indexer: indexer_id!("4e5c87772c29381bcabc58c3f182b6633b5a274a"),
                            },
                            AllocationInfo {
                                id: allocation_id!("2e9e707f8dfea2f03ef194c1b6478845377e6246"),
                                indexer: indexer_id!("bdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491"),
                            },
                            AllocationInfo {
                                id: allocation_id!("3c4a845623182c6cffe0da2c8f6d9e9128f34208"),
                                indexer: indexer_id!("269ebeee083ce6f70486a67dc8036a889bf322a9"),
                            },
                        ],
                        manifest_network: "arbitrum-one".to_string(),
                        manifest_start_block: 42440000,
                        subgraphs: HashSet::from([subgraph_id]),
                        transferred_to_l2: false,
                    },
                },
                SubgraphVersionRawInfo {
                    version: 0,
                    deployment: DeploymentRawInfo {
                        id: deployment_v100,
                        allocations: vec![AllocationInfo {
                            id: allocation_id!("89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2"),
                            indexer: indexer_id!("bdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491"),
                        }],
                        manifest_network: "arbitrum-one".to_string(),
                        manifest_start_block: 42440000,
                        subgraphs: HashSet::from([subgraph_id]),
                        transferred_to_l2: false,
                    },
                },
            ],
        },
    )]);

    //* When
    let info = subgraph_processing::process_subgraph_info(raw_info);

    //* Then
    // Assert subgraphs processed info
    assert_eq!(info.len(), 1);
    assert!(info.contains_key(&subgraph_id));

    let subgraph_info = info
        .get(&subgraph_id)
        .expect("subgraph info not found")
        .as_ref()
        .expect("subgraph blocked");
    assert_eq!(subgraph_info.versions.len(), 2);

    let version_1 = &subgraph_info.versions[0]; // Highest version
    assert_eq!(version_1.version, 1);
    assert_eq!(version_1.deployment_id, deployment_v110);

    assert_matches!(&version_1.deployment, Ok(deployment) => {
        assert_eq!(deployment.manifest_network, "arbitrum-one");
        assert_eq!(deployment.manifest_start_block, 42440000);

        assert_eq!(deployment.subgraphs.len(), 1);
        assert!(deployment.subgraphs.contains(&subgraph_id));

        assert!(!deployment.allocations.is_empty());
    });

    let version_0 = &subgraph_info.versions[1];
    assert_eq!(version_0.version, 0);
    assert_eq!(version_0.deployment_id, deployment_v100);

    assert_matches!(&version_0.deployment, Ok(deployment) => {
        assert_eq!(deployment.manifest_network, "arbitrum-one");
        assert_eq!(deployment.manifest_start_block, 42440000);

        assert_eq!(deployment.subgraphs.len(), 1);
        assert!(deployment.subgraphs.contains(&subgraph_id));

        assert!(!deployment.allocations.is_empty());
    });
}

#[test]
fn block_deployment_when_no_allocations() {
    init_test_tracing();

    //* Given
    let deployment_v001 = deployment_id!("QmU318BETTzmjUhBMDndQEaGqyP4rCSbiSZBZapaqNQQfF");
    let raw_info = HashMap::from([(
        deployment_v001,
        DeploymentRawInfo {
            id: deployment_v001,
            manifest_network: "mainnet".to_string(),
            manifest_start_block: 15685263,
            subgraphs: Default::default(),
            transferred_to_l2: false, // Not marked as transferred
            allocations: vec![],      // No allocations
        },
    )]);

    //* When
    let info = subgraph_processing::process_deployments_info(raw_info);

    //* Then
    // Assert deployments processed info
    assert_eq!(info.len(), 1);
    assert!(info.contains_key(&deployment_v001));

    let deployment_err = info
        .get(&deployment_v001)
        .expect("deployment info not found")
        .as_ref()
        .expect_err("deployment not blocked");

    assert_matches!(deployment_err, &DeploymentError::NoAllocations);
}

#[test]
fn block_subgraph_when_all_deployments_have_been_transferred_to_l2() {
    init_test_tracing();

    //* Given
    // Graph Network Subgraph info (on 2024-05-27)
    let subgraph_id = subgraph_id!("2ko2nM7rMkL4BmFbnMoAatb69EcA8MBApAPTorDVNTgj");
    let subgraph_id_on_l2 = subgraph_id!("3uQzo8AbYn9Pwdp5aEuBQaocu7FtdVwZUV72aJGL5Gik");
    let deployment_v001 = deployment_id!("QmU318BETTzmjUhBMDndQEaGqyP4rCSbiSZBZapaqNQQfF");
    let raw_info = HashMap::from([(
        subgraph_id,
        SubgraphRawInfo {
            id: subgraph_id,
            id_on_l2: Some(subgraph_id_on_l2),
            versions: vec![SubgraphVersionRawInfo {
                version: 0,
                deployment: DeploymentRawInfo {
                    id: deployment_v001,
                    manifest_network: "mainnet".to_string(),
                    manifest_start_block: 15685263,
                    subgraphs: Default::default(),
                    transferred_to_l2: true, // Marked as transferred
                    allocations: vec![],     // No allocations
                },
            }],
        },
    )]);

    //* When
    let info = subgraph_processing::process_subgraph_info(raw_info);

    //* Then
    // Assert subgraphs processed info
    assert_eq!(info.len(), 1);
    assert!(info.contains_key(&subgraph_id));

    let subgraph_err = info
        .get(&subgraph_id)
        .expect("subgraph info not found")
        .as_ref()
        .expect_err("subgraph not blocked");

    assert_matches!(subgraph_err, &SubgraphError::TransferredToL2 { id_on_l2: Some(id_on_l2) } => {
        assert_eq!(id_on_l2, subgraph_id_on_l2);
    });
}

#[test]
fn block_subgraph_when_all_deployments_have_no_allocations() {
    init_test_tracing();

    //* Given
    let subgraph_id = subgraph_id!("2ko2nM7rMkL4BmFbnMoAatb69EcA8MBApAPTorDVNTgj");
    let subgraph_id_on_l2 = subgraph_id!("3uQzo8AbYn9Pwdp5aEuBQaocu7FtdVwZUV72aJGL5Gik");
    let deployment_v001 = deployment_id!("QmU318BETTzmjUhBMDndQEaGqyP4rCSbiSZBZapaqNQQfF");
    let raw_info = HashMap::from([(
        subgraph_id,
        SubgraphRawInfo {
            id: subgraph_id,
            id_on_l2: Some(subgraph_id_on_l2),
            versions: vec![SubgraphVersionRawInfo {
                version: 0,
                deployment: DeploymentRawInfo {
                    id: deployment_v001,
                    manifest_network: "mainnet".to_string(),
                    manifest_start_block: 15685263,
                    subgraphs: Default::default(),
                    transferred_to_l2: false, // Not marked as transferred
                    allocations: vec![],      // No allocations
                },
            }],
        },
    )]);

    //* When
    let info = subgraph_processing::process_subgraph_info(raw_info);

    //* Then
    // Assert subgraphs processed info
    assert_eq!(info.len(), 1);
    assert!(info.contains_key(&subgraph_id));

    let subgraph_err = info
        .get(&subgraph_id)
        .expect("subgraph info not found")
        .as_ref()
        .expect_err("subgraph not blocked");

    assert_matches!(subgraph_err, &SubgraphError::NoAllocations);
}

#[test]
fn block_subgraph_deployment_if_marked_as_transferred_to_l2() {
    init_test_tracing();

    //* Given
    let subgraph_id = subgraph_id!("2ko2nM7rMkL4BmFbnMoAatb69EcA8MBApAPTorDVNTgj");
    let deployment_v003 = deployment_id!("QmU318BETTzmjUhBMDndQEaGqyP4rCSbiSZBZapaqNQQfF");
    let deployment_v002 = deployment_id!("QmPK1s3pNYLi9ERiq3BDxKa4XosgWwFRQUydHUtz4YgpqB");
    let raw_info = HashMap::from([(
        subgraph_id,
        SubgraphRawInfo {
            id: subgraph_id,
            id_on_l2: None,
            versions: vec![
                SubgraphVersionRawInfo {
                    version: 2,
                    deployment: DeploymentRawInfo {
                        id: deployment_v003,
                        manifest_network: "mainnet".to_string(),
                        manifest_start_block: 15685263,
                        subgraphs: Default::default(),
                        transferred_to_l2: false,
                        allocations: vec![AllocationInfo {
                            id: allocation_id!("177b557b12f22bb17a9d73dcc994d978dd6f5f89"),
                            indexer: indexer_id!("4e5c87772c29381bcabc58c3f182b6633b5a274a"),
                        }],
                    },
                },
                SubgraphVersionRawInfo {
                    version: 1,
                    deployment: DeploymentRawInfo {
                        id: deployment_v002,
                        manifest_network: "mainnet".to_string(),
                        manifest_start_block: 15685263,
                        subgraphs: Default::default(),
                        transferred_to_l2: true, // Marked as transferred
                        allocations: vec![],     // No allocations
                    },
                },
            ],
        },
    )]);

    //* When
    let info = subgraph_processing::process_subgraph_info(raw_info);

    //* Then
    // Assert subgraphs processed info
    assert_eq!(info.len(), 1);
    assert!(info.contains_key(&subgraph_id));

    let subgraph_info = info
        .get(&subgraph_id)
        .expect("subgraph info not found")
        .as_ref()
        .expect("subgraph blocked");

    assert_eq!(subgraph_info.versions.len(), 2);

    let version_2 = &subgraph_info.versions[0]; // Highest version
    assert_eq!(version_2.version, 2);
    assert_eq!(version_2.deployment_id, deployment_v003);
    assert!(version_2.deployment.is_ok());

    let version_1 = &subgraph_info.versions[1];
    assert_eq!(version_1.version, 1);
    assert_eq!(version_1.deployment_id, deployment_v002);
    assert_matches!(version_1.deployment, Err(DeploymentError::TransferredToL2));
}

#[test]
fn block_subgraph_deployment_if_has_no_allocations() {
    init_test_tracing();

    //* Given
    let subgraph_id = subgraph_id!("2ko2nM7rMkL4BmFbnMoAatb69EcA8MBApAPTorDVNTgj");
    let deployment_v003 = deployment_id!("QmU318BETTzmjUhBMDndQEaGqyP4rCSbiSZBZapaqNQQfF");
    let deployment_v002 = deployment_id!("QmPK1s3pNYLi9ERiq3BDxKa4XosgWwFRQUydHUtz4YgpqB");
    let raw_info = HashMap::from([(
        subgraph_id,
        SubgraphRawInfo {
            id: subgraph_id,
            id_on_l2: None,
            versions: vec![
                SubgraphVersionRawInfo {
                    version: 2,
                    deployment: DeploymentRawInfo {
                        id: deployment_v003,
                        manifest_network: "mainnet".to_string(),
                        manifest_start_block: 15685263,
                        subgraphs: Default::default(),
                        transferred_to_l2: false,
                        allocations: vec![AllocationInfo {
                            id: allocation_id!("177b557b12f22bb17a9d73dcc994d978dd6f5f89"),
                            indexer: indexer_id!("4e5c87772c29381bcabc58c3f182b6633b5a274a"),
                        }],
                    },
                },
                SubgraphVersionRawInfo {
                    version: 1,
                    deployment: DeploymentRawInfo {
                        id: deployment_v002,
                        manifest_network: "mainnet".to_string(),
                        manifest_start_block: 15685263,
                        subgraphs: Default::default(),
                        transferred_to_l2: false, // Not marked as transferred
                        allocations: vec![],      // No allocations
                    },
                },
            ],
        },
    )]);

    //* When
    let info = subgraph_processing::process_subgraph_info(raw_info);

    //* Then
    // Assert subgraphs processed info
    assert_eq!(info.len(), 1);
    assert!(info.contains_key(&subgraph_id));

    let subgraph_info = info
        .get(&subgraph_id)
        .expect("subgraph info not found")
        .as_ref()
        .expect("subgraph blocked");

    assert_eq!(subgraph_info.versions.len(), 2);

    let version_2 = &subgraph_info.versions[0]; // Highest version
    assert_eq!(version_2.version, 2);
    assert_eq!(version_2.deployment_id, deployment_v003);
    assert!(version_2.deployment.is_ok());

    let version_1 = &subgraph_info.versions[1];
    assert_eq!(version_1.version, 1);
    assert_eq!(version_1.deployment_id, deployment_v002);
    assert_matches!(version_1.deployment, Err(DeploymentError::NoAllocations));
}
