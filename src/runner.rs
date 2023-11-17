use anyhow::Context;
use libsql::Database;
use tokio::task::JoinSet;

use crate::Result;

pub struct Runner<T> {
    url: String,
    token: String,
    job: T,
}

impl<T: Job> Runner<T> {
    pub fn new(url: String, token: String, job: T) -> Self {
        Self { url, token, job }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut set = JoinSet::new();

        self.job
            .run(&self.url, &self.token, &mut set)
            .context("failed to setup job")?;

        while let Some(Ok(v)) = set.join_next().await {
            if let Err(e) = v {
                tracing::error!("Task failed: {:?}", e);
            }
        }

        println!("Job finished executing");

        Ok(())
    }
}

pub trait Job {
    fn run(&mut self, url: &str, token: &str, set: &mut JoinSet<Result<()>>) -> Result<()>;
}

pub struct SingleConn {}

impl Job for SingleConn {
    fn run(&mut self, url: &str, token: &str, set: &mut JoinSet<Result<()>>) -> Result<()> {
        let db = Database::open_remote(url, token).context("open remote")?;

        set.spawn(async move {
            let conn = db.connect().context("connect")?;

            conn.execute("CREATE TABLE IF NOT EXISTS foo (x BLOB)", ())
                .await
                .context("create")?;

            for _ in 0..50_000 {
                conn.execute("INSERT INTO foo values (randomblob(6000))", ())
                    .await
                    .context("insert")?;
            }

            Ok(())
        });

        Ok(())
    }
}
