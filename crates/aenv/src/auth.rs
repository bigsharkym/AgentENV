use anyhow::{bail, Context, Result};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credentials {
    pub url: String,
    pub api_key: String,
}

const ENV_SERVER_URL: &str = "AENV_SERVER_URL";
const ENV_SERVER_URL_ALIAS: &str = "AENV_URL";
const ENV_API_KEY: &str = "AENV_API_KEY";

fn credentials_path() -> Result<PathBuf> {
    let dirs = ProjectDirs::from("", "", "aenv").context("could not determine config directory")?;
    Ok(dirs.config_dir().join("credentials"))
}

fn env_value(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Load credentials, preferring the environment over the credentials file.
///
/// `AENV_SERVER_URL` (or its alias `AENV_URL`) and `AENV_API_KEY` override the
/// stored file, so a deployment can export a single variable without having to
/// rewrite `~/.config/aenv/credentials`.
pub fn load() -> Result<Credentials> {
    let env_url = env_value(ENV_SERVER_URL).or_else(|| env_value(ENV_SERVER_URL_ALIAS));
    let env_api_key = env_value(ENV_API_KEY);

    if let (Some(url), Some(api_key)) = (env_url.clone(), env_api_key.clone()) {
        return Ok(Credentials { url, api_key });
    }

    let path = credentials_path()?;
    let text = match fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let expected = if env_url.is_some() || env_api_key.is_some() {
                format!(
                    "incomplete credentials — set both {ENV_SERVER_URL} (or {ENV_SERVER_URL_ALIAS}) and {ENV_API_KEY}"
                )
            } else {
                format!(
                    "set {ENV_SERVER_URL} and {ENV_API_KEY}, or run `aenv auth`"
                )
            };
            bail!(
                "not authenticated — {expected} (expected credentials at {})",
                path.display()
            );
        }
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let mut creds: Credentials =
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    if let Some(url) = env_url {
        creds.url = url;
    }
    if let Some(api_key) = env_api_key {
        creds.api_key = api_key;
    }
    Ok(creds)
}

pub fn save(creds: &Credentials) -> Result<()> {
    let path = credentials_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    let text = toml::to_string(creds)?;
    fs::write(&path, text).with_context(|| format!("writing {}", path.display()))?;
    restrict_permissions(&path)?;
    Ok(())
}

#[cfg(unix)]
fn restrict_permissions(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(0o600);
    fs::set_permissions(path, perms)?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &std::path::Path) -> Result<()> {
    Ok(())
}
