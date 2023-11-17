use crate::Result;
use anyhow::Context;
use tokio::process::Command;

pub async fn db_url(namespace: &str) -> Result<String> {
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

pub async fn db_token(namespace: &str) -> Result<String> {
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
