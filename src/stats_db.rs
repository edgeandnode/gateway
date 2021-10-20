use crate::{prelude::*, query_engine::APIKey};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use std::{
    collections::{hash_map::Entry, HashMap},
    error::Error,
    iter::FromIterator as _,
    sync::Arc,
    time::SystemTime,
};
use tokio::{
    self,
    time::{interval, Interval},
};
use tokio_postgres::{self, types::Type};

pub enum Msg {
    AddQuery {
        api_key: Arc<APIKey>,
        fee: GRT,
        domain: String,
        subgraph: Option<String>,
    },
}

struct Client {
    client: tokio_postgres::Client,
    msgs: mpsc::UnboundedReceiver<Msg>,
    flush_interval: Interval,
    api_key_stats: HashMap<String, APIKeyStats>,
    update_statements: [tokio_postgres::Statement; 3],
}

struct APIKeyStats {
    api_key: Arc<APIKey>,
    stats: Stats,
    domains: HashMap<i32, Stats>,
    subgraphs: HashMap<i32, Stats>,
}

struct Stats {
    queries: u64,
    fees: GRT,
    time: SystemTime,
}

impl Default for Stats {
    fn default() -> Self {
        Self::new(GRT::zero())
    }
}

impl Stats {
    fn new(fee: GRT) -> Self {
        Self {
            queries: 1,
            fees: fee,
            time: SystemTime::now(),
        }
    }

    fn add(&mut self, fee: GRT) {
        self.queries += 1;
        self.fees += fee;
        self.time = SystemTime::now();
    }

    fn clear(&mut self) {
        self.queries = 0;
        self.fees = GRT::zero();
    }
}

pub async fn create(
    host: &str,
    port: u16,
    dbname: &str,
    user: &str,
    password: &str,
) -> Result<mpsc::UnboundedSender<Msg>, Box<dyn Error>> {
    let config = format!(
        "host={} port={} user={} password={} dbname={} sslmode=prefer",
        host, port, user, password, dbname
    );
    let mut ssl_builder = SslConnector::builder(SslMethod::tls()).unwrap();
    ssl_builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(ssl_builder.build());
    let (client, connection) = tokio_postgres::connect(&config, connector).await?;
    // The connection object performs the actual communication with the database and is intended to
    // be executed on its own spawned task.
    tokio::spawn(async move {
        if let Err(connection_err) = connection.await {
            tracing::error!(%connection_err);
        }
    });
    let update_statements = init_tables(&client).await?;
    let (send, recv) = mpsc::unbounded_channel();
    let mut client = Client {
        client,
        msgs: recv,
        flush_interval: interval(Duration::from_secs(30)),
        api_key_stats: HashMap::new(),
        update_statements,
    };
    tokio::spawn(async move { while let Ok(()) = client.run().await {} });
    Ok(send)
}

async fn init_tables(
    client: &tokio_postgres::Client,
) -> Result<[tokio_postgres::Statement; 3], Box<dyn Error>> {
    let table_specs = [
        ("api_key_stats", "userId"),
        ("authorized_domain_stats", "apiKeyId"),
        ("authorized_subgraph_stats", "apiKeyId"),
    ];
    let init_tables_sql = table_specs
        .iter()
        .map(|(table_name, id2)| {
            format!(
                r#"
                CREATE TABLE IF NOT EXISTS {0} (
                    id INT4,
                    "{1}" INT4,
                    key CHAR(255),
                    "ethAddress" CHAR(42),
                    queries INT8,
                    fees NUMERIC,
                    time TIMESTAMPTZ PRIMARY KEY,
                    UNIQUE (time, id));
                SELECT create_hypertable('{0}', 'time', if_not_exists => TRUE);
                CREATE INDEX IF NOT EXISTS {0}_id_time_idx ON {0} (id, time DESC);
                CREATE INDEX IF NOT EXISTS {0}_user_id_time_idx ON {0} ("{1}", time DESC);
                "#,
                table_name, id2,
            )
        })
        .collect::<Vec<String>>()
        .join("");
    client.batch_execute(&init_tables_sql).await?;
    Ok([
        prepare_statement(client, table_specs[0]).await?,
        prepare_statement(client, table_specs[1]).await?,
        prepare_statement(client, table_specs[2]).await?,
    ])
}

async fn prepare_statement(
    client: &tokio_postgres::Client,
    spec: (&str, &str),
) -> Result<tokio_postgres::Statement, Box<dyn Error>> {
    let sql = format!(
        r#"
            INSERT INTO {0}
                (id, "{1}", key, "ethAddress", queries, fees, time)
            VALUES
                ($1, $2, $3, $4, $5, CAST($6 AS NUMERIC), $7)
            ON CONFLICT (time, id) DO UPDATE SET
                queries = {0}.queries + EXCLUDED.queries,
                fees = {0}.fees + EXCLUDED.fees;
            "#,
        spec.0, spec.1,
    );
    client
        .prepare_typed(
            &sql,
            &[
                Type::INT4,
                Type::INT4,
                Type::BPCHAR,
                Type::BPCHAR,
                Type::INT8,
                Type::TEXT,
                Type::TIMESTAMPTZ,
            ],
        )
        .await
        .map_err(|err| err.into())
}

impl Client {
    async fn run(&mut self) -> Result<(), ()> {
        tokio::select! {
            msg = self.msgs.recv() => self.handle_msg(msg.ok_or(())?).await,
            _ = self.flush_interval.tick() => {
                let _ = self.flush().await;
            },
        };
        Ok(())
    }

    async fn handle_msg(&mut self, msg: Msg) {
        match msg {
            Msg::AddQuery {
                api_key,
                fee,
                domain,
                subgraph,
            } => {
                let domain_id = api_key
                    .domains
                    .iter()
                    .find(|(d, _)| d == &domain)
                    .map(|(_, id)| *id);
                let subgraph_id = subgraph.and_then(|subgraph| {
                    api_key
                        .subgraphs
                        .iter()
                        .find(|(s, _)| s == &subgraph)
                        .map(|(_, id)| *id)
                });
                match self.api_key_stats.entry(api_key.key.clone()) {
                    Entry::Vacant(entry) => {
                        entry.insert(APIKeyStats {
                            api_key,
                            stats: Stats::new(fee),
                            domains: HashMap::from_iter(
                                domain_id.into_iter().map(|d| (d, Stats::new(fee))),
                            ),
                            subgraphs: HashMap::from_iter(
                                subgraph_id.into_iter().map(|s| (s, Stats::new(fee))),
                            ),
                        });
                    }
                    Entry::Occupied(entry) => {
                        let entry = entry.into_mut();
                        entry.stats.add(fee);
                        if let Some(domain_id) = domain_id {
                            entry.domains.entry(domain_id).or_default().add(fee);
                        }
                        if let Some(subgraph_id) = subgraph_id {
                            entry.subgraphs.entry(subgraph_id).or_default().add(fee);
                        }
                    }
                };
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn flush(&mut self) -> Result<(), ()> {
        tracing::info!(api_key_stats_updates = %self.api_key_stats.len());
        if self.api_key_stats.is_empty() {
            return Ok(());
        }
        for (_, api_key_stats) in &mut self.api_key_stats {
            let api_key_id = api_key_stats.api_key.id as i32;
            Self::execute_update(
                &self.client,
                &self.update_statements[0],
                &api_key_stats.api_key,
                &api_key_stats.stats,
                api_key_id,
                api_key_stats.api_key.user_id as i32,
            )
            .await?;
            // Clear stats to avoid double counting if future updates fail.
            api_key_stats.stats.clear();
            for (domain, stats) in &mut api_key_stats.domains {
                Self::execute_update(
                    &self.client,
                    &self.update_statements[1],
                    &api_key_stats.api_key,
                    stats,
                    *domain,
                    api_key_id,
                )
                .await?;
                stats.clear();
            }
            for (subgraph, stats) in &mut api_key_stats.subgraphs {
                Self::execute_update(
                    &self.client,
                    &self.update_statements[2],
                    &api_key_stats.api_key,
                    stats,
                    *subgraph,
                    api_key_id,
                )
                .await?;
                stats.clear();
            }
        }
        self.api_key_stats.clear();
        Ok(())
    }

    #[tracing::instrument(skip(client, statement, stats, key1, key2))]
    async fn execute_update(
        client: &tokio_postgres::Client,
        statement: &tokio_postgres::Statement,
        api_key: &APIKey,
        stats: &Stats,
        key1: i32,
        key2: i32,
    ) -> Result<(), ()> {
        if let Err(execute_update_err) = client
            .execute(
                statement,
                &[
                    &key1,
                    &key2,
                    &api_key.key,
                    &api_key.user_address.to_string(),
                    &(stats.queries as i64),
                    &stats.fees.to_string(),
                    &stats.time,
                ],
            )
            .await
        {
            tracing::error!(%execute_update_err);
            return Err(());
        }
        Ok(())
    }
}
