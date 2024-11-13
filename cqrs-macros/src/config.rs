use cfg_if::cfg_if;
use serde::Deserialize;
use std::{
    env,
    io::{self, Write},
    path::PathBuf,
};

const MAX_1_BYTE_PROTOBUF_TAG: u32 = 15;

#[inline]
const fn default_protobuf_version_tag() -> u32 {
    MAX_1_BYTE_PROTOBUF_TAG
}

fn default_protobuf_version() -> Field {
    Field {
        tag: MAX_1_BYTE_PROTOBUF_TAG,
    }
}

#[derive(Debug, Deserialize)]
pub struct Field {
    #[serde(default = "default_protobuf_version_tag")]
    pub tag: u32,
}

cfg_if! {
    if #[cfg(feature = "prost")] {
        #[derive(Debug, Deserialize)]
        pub struct Prost {
            pub version: Field,
        }

        impl Default for Prost {
            fn default() -> Self {
                Self {
                    version: default_protobuf_version(),
                }
            }
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct Fields {
    #[cfg(feature = "prost")]
    pub prost: Prost,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub fields: Fields,
}

cfg_if! {
    if #[cfg(feature = "prost")] {
        impl Default for Config {
            fn default() -> Self {
                Self {
                    fields: Fields {
                        prost: Prost::default(),
                    },
                }
            }
        }
    } else {
        impl Default for Config {
            fn default() -> Self {
                Self { fields: Default::default() }
            }
        }
    }
}

#[derive(Deserialize)]
struct Sections {
    #[serde(default)]
    cqrs: Config,
}

fn resolve_from_path(path: Option<PathBuf>, names: &[&str]) -> Option<PathBuf> {
    if let Some(path) = path {
        for name in names {
            let file = path.join(".cargo").join(name);

            if file.exists() {
                return Some(file);
            }
        }
    }

    None
}

#[inline]
fn resolve_from_env_var(key: &str, names: &[&str]) -> Option<PathBuf> {
    resolve_from_path(env::var_os(key).map(PathBuf::from), names)
}

// HACK: there's currently no built-in way to get the workspace directory
// REF: https://github.com/rust-lang/cargo/issues/3946
fn workspace_dir() -> Option<PathBuf> {
    let root = env::current_dir().map(PathBuf::from).ok()?;
    let ancestors = root.ancestors();

    for ancestor in ancestors {
        let cargo_lock = ancestor.join("Cargo.lock");

        if cargo_lock.exists() {
            return Some(ancestor.to_path_buf());
        }
    }

    None
}

fn resolve_config_file() -> Option<PathBuf> {
    let files = ["config", "config.toml"];

    resolve_from_env_var("CARGO_MANIFEST_DIR", &files)
        .or_else(|| resolve_from_path(env::current_dir().map(PathBuf::from).ok(), &files))
        .or_else(|| resolve_from_path(workspace_dir(), &files))
}

fn try_get() -> Option<Config> {
    let config_path = resolve_config_file()?;
    let content = fs_err::read_to_string(&config_path).ok()?;
    let config: Sections = match toml::from_str(&content) {
        Ok(config) => config,
        Err(err) => {
            let _ = writeln!(io::stderr(), "Warning: {}: {}", config_path.display(), err);
            None
        }
    }?;

    Some(config.cqrs)
}

pub fn get() -> Config {
    try_get().unwrap_or_default()
}
