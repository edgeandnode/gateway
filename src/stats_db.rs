use crate::{prelude::*, query_engine::APIKey};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
    time::SystemTime,
};
use tokio::{
    self,
    time::{interval, Interval},
};
use tokio_postgres::{self, types::Type, NoTls};

pub enum Msg {
    AddQuery { api_key: Arc<APIKey>, fee: GRT },
}

struct Client {
    client: tokio_postgres::Client,
    msgs: mpsc::UnboundedReceiver<Msg>,
    flush_interval: Interval,
    api_key_stats: HashMap<String, APIKeyStats>,
    api_key_update: tokio_postgres::Statement,
}

pub async fn create(
    host: &str,
    dbname: &str,
    user: &str,
    password: &str,
) -> Option<mpsc::UnboundedSender<Msg>> {
    let config = format!(
        "host={} user={} password={} dbname={}",
        host, user, password, dbname
    );
    let (client, connection) = match tokio_postgres::connect(&config, NoTls).await {
        Ok(result) => result,
        Err(postgres_connect_err) => {
            tracing::error!(%postgres_connect_err);
            return None;
        }
    };
    tokio::spawn(async move {
        if let Err(connection_err) = connection.await {
            tracing::error!(%connection_err);
        }
    });
    if let Err(create_table_err) = client
        .batch_execute(
            r#"
            CREATE TABLE IF NOT EXISTS api_key_stats (
                id INT4,
                key CHAR(255),
                "userId" INT4,
                "ethAddress" CHAR(42),
                queries INT8,
                fees NUMERIC,
                time TIMESTAMPTZ PRIMARY KEY,
                UNIQUE (time, id));
            SELECT create_hypertable('api_key_stats', 'time', if_not_exists => TRUE);
            CREATE INDEX IF NOT EXISTS api_key_stats_id_time_idx
                ON api_key_stats (id, time DESC);
            CREATE INDEX IF NOT EXISTS api_key_stats_user_id_time_idx
                ON api_key_stats ("userId", time DESC);
            "#,
        )
        .await
    {
        tracing::error!(%create_table_err);
        return None;
    };
    let api_key_update = match client
        .prepare_typed(
            r#"
            INSERT INTO api_key_stats
                (id, key, "userId", "ethAddress", queries, fees, time)
            VALUES
                ($1, $2, $3, $4, $5, CAST($6 AS NUMERIC), $7)
            ON CONFLICT (time, id) DO UPDATE SET
                queries = api_key_stats.queries + EXCLUDED.queries,
                fees = api_key_stats.fees + EXCLUDED.fees;
            "#,
            &[
                Type::INT4,
                Type::TEXT,
                Type::INT4,
                Type::BPCHAR,
                Type::INT8,
                Type::TEXT,
                Type::TIMESTAMPTZ,
            ],
        )
        .await
    {
        Ok(statement) => statement,
        Err(prepare_statement_err) => {
            tracing::error!(%prepare_statement_err);
            return None;
        }
    };
    let (send, recv) = mpsc::unbounded_channel();
    let mut client = Client {
        client,
        msgs: recv,
        api_key_stats: HashMap::new(),
        flush_interval: interval(Duration::from_secs(30)),
        api_key_update,
    };
    tokio::spawn(async move { while let Ok(()) = client.run().await {} });
    Some(send)
}

pub struct APIKeyStats {
    api_key: Arc<APIKey>,
    queries: u64,
    fees: GRT,
    time: SystemTime,
}

impl Client {
    async fn run(&mut self) -> Result<(), ()> {
        tokio::select! {
            msg = self.msgs.recv() => match msg {
                Some(msg) => self.handle_msg(msg).await,
                None => return Err(()),
            },
            _ = self.flush_interval.tick() => self.flush().await,
        };
        Ok(())
    }

    async fn handle_msg(&mut self, msg: Msg) {
        match msg {
            Msg::AddQuery { api_key, fee, .. } => {
                match self.api_key_stats.entry(api_key.key.clone()) {
                    Entry::Vacant(entry) => {
                        entry.insert(APIKeyStats {
                            api_key,
                            queries: 1,
                            fees: fee,
                            time: SystemTime::now(),
                        });
                    }
                    Entry::Occupied(entry) => {
                        let entry = entry.into_mut();
                        entry.queries += 1;
                        entry.fees += fee;
                        entry.time = SystemTime::now();
                    }
                };
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn flush(&mut self) {
        tracing::info!(api_key_stats_updates = %self.api_key_stats.len());
        if self.api_key_stats.is_empty() {
            return;
        }
        for (key, stats) in &self.api_key_stats {
            if let Err(execute_err) = self
                .client
                .execute(
                    &self.api_key_update,
                    &[
                        &(stats.api_key.id as i32),
                        key,
                        &(stats.api_key.user_id as i32),
                        &stats.api_key.user_address.to_string(),
                        &(stats.queries as i64),
                        &stats.fees.to_string(),
                        &stats.time,
                    ],
                )
                .await
            {
                tracing::error!(%execute_err);
                break;
            };
        }
        self.api_key_stats.clear();
    }
}
