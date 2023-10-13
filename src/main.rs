use itertools::Itertools;
use std::{
    collections::VecDeque,
    io::{BufRead, Read},
};
use tokio::task::JoinSet;

use anyhow::{Context, Result};
use bytes::Buf;
use hyper::{client::HttpConnector, Body, Request, StatusCode};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut set = JoinSet::new();

    let client = Client::default();

    let stmts = load_file("speedtest1.sql")?;

    //     let stmts = stmts
    //         .into_iter()
    //         .batching(|it| Some(it.take(50).collect::<Vec<_>>()))
    //         .collect::<Vec<_>>();

    for i in 0..50 {
        let namespace = format!("4ar-{}", i);
        let fut = run(client.clone(), namespace, stmts.clone());

        set.spawn(async move {
            if let Err(e) = fut.await {
                println!("failed task: {:?}", e);
            }
        });
    }

    while let Some(_) = set.join_next().await {}

    println!("done");

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
