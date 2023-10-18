use libsql::Database;
use std::{
    collections::VecDeque,
    env,
    io::{BufRead, Read},
};
use tokio::{process::Command, task::JoinSet};

use anyhow::{Context, Result};
use bytes::Buf;
use hyper::{client::HttpConnector, Body, Request, StatusCode};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut set = JoinSet::new();

    let task = env::args().nth(1);

    match task.as_deref() {
        Some("build") => run_local(&mut set).await?,
        Some("fly") => run_fly(&mut set).await?,
        _ => todo!("help page"),
    }

    while let Some(_) = set.join_next().await {}

    println!("done");

    Ok(())
}

async fn run_fly(set: &mut JoinSet<()>) -> Result<()> {
    let stmts = load_file("speedtest1.sql")?;

    let namespaces = ["bar1"];

    for namespace in namespaces {
        let stmts = stmts.clone();
        set.spawn(async move {
            if let Err(e) = run_fly_namespace(namespace, stmts).await {
                eprintln!("error: {:?}", e);
            }
        });
    }

    Ok(())
}

async fn run_fly_namespace(namespace: &str, stmts: VecDeque<String>) -> Result<()> {
    // let url = turso_db_url(namespace).await.context("show url")?;
    // let token = turso_db_token(namespace).await.context("create token")?;
    let url = "http://localhost:8080";
    let token = "";

    let db = Database::open_remote(url, token).context("open remote")?;

    let conn = db.connect().context("connect")?;

    for stmt in stmts.into_iter().take(150) {
        if let Err(e) = conn
            .execute(&stmt, ())
            .await
            .with_context(|| format!("execute failed with: {}", stmt))
        {
            if !format!("{:?}", e).contains("already exists") {
                eprintln!("{:?}", e);
            }
        };
    }

    conn.execute("SELECT 1", ())
        .await
        .context("execute SELECT 1")?;

    Ok(())
}

async fn turso_db_url(namespace: &str) -> Result<String> {
    let out = Command::new("turso")
        .arg("db")
        .arg("show")
        .arg("--url")
        .arg(namespace)
        .output()
        .await
        .context("turso db show --url")?;

    if !out.status.success() {
        let msg = String::from_utf8(out.stderr)?;
        anyhow::bail!("non-zero exit: {} msg: {}", out.status, msg)
    }

    let out = String::from_utf8(out.stdout)?;

    Ok(out.replace("\n", ""))
}

async fn turso_db_token(namespace: &str) -> Result<String> {
    let out = Command::new("turso")
        .arg("db")
        .arg("tokens")
        .arg("create")
        .arg(namespace)
        .output()
        .await
        .context("turso db create token")?;

    if !out.status.success() {
        let msg = String::from_utf8(out.stderr)?;
        anyhow::bail!("non-zero exit: {} msg: {}", out.status, msg)
    }

    Ok(String::from_utf8(out.stdout)?.replace("\n", ""))
}

async fn run_local(set: &mut JoinSet<()>) -> Result<()> {
    let client = Client::default();

    let stmts = load_file("speedtest1.sql")?;

    for i in 0..50 {
        let namespace = format!("4ar-{}", i);
        let fut = run(client.clone(), namespace, stmts.clone());

        set.spawn(async move {
            if let Err(e) = fut.await {
                println!("failed task: {:?}", e);
            }
        });
    }

    Ok(())
}

async fn run(client: Client, namespace: String, mut stmts: VecDeque<String>) -> Result<()> {
    client
        .create_namespace(&namespace)
        .await
        .context("create namespace")?;

    let mut done = false;

    loop {
        let mut stmt = Vec::with_capacity(50);

        for _ in 0..50 {
            if let Some(s) = stmts.pop_front() {
                stmt.push(s);
            } else {
                done = true;
            }
        }

        client
            .do_inserts(&namespace, stmt)
            .await
            .context("inserts")?;

        if done {
            break;
        }
    }

    Ok(())
}

fn load_file(path: impl AsRef<std::path::Path>) -> Result<VecDeque<String>> {
    let f = std::fs::File::open(path)?;
    let f = std::io::BufReader::new(f);

    let lines = f
        .lines()
        .filter(|l| match l {
            Ok(l) => !(l.starts_with("BEGIN") || l.starts_with("COMMIT") || l.starts_with("--")),
            Err(_) => true,
        })
        .collect::<Result<VecDeque<_>, _>>()?;

    Ok(lines)
}

#[derive(Clone, Default)]
struct Client {
    client: hyper::Client<HttpConnector>,
}

impl Client {
    async fn create_namespace(&self, name: &str) -> Result<()> {
        let json = serde_json::json!({});

        let req = Request::post(format!(
            "http://localhost:8081/v1/namespaces/{}/create",
            name
        ))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&json)?))
        .unwrap();

        let res = self.client.request(req).await?;

        if res.status() == StatusCode::OK || res.status() == StatusCode::BAD_REQUEST {
            return Ok(());
        }

        let (res, body) = res.into_parts();
        let b = hyper::body::aggregate(body).await?;
        let mut s = String::new();
        b.reader().read_to_string(&mut s)?;
        anyhow::bail!("failed request: {:?}, {:?}", res, s);
    }

    async fn do_inserts(&self, name: &str, stmts: Vec<String>) -> Result<()> {
        let json = serde_json::json!({
            "statements": stmts
        });

        let req = Request::post("http://localhost:8080")
            .header("host", format!("{}.foo", name))
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&json)?))
            .unwrap();

        self.send(req).await?;

        Ok(())
    }

    async fn send(&self, req: Request<Body>) -> Result<()> {
        let res = self.client.request(req).await?;

        if res.status() != StatusCode::OK {
            let (res, body) = res.into_parts();
            let b = hyper::body::aggregate(body).await?;
            let mut s = String::new();
            b.reader().read_to_string(&mut s)?;
            anyhow::bail!("failed request: {:?}, {:?}", res, s);
        }

        Ok(())
    }
}
