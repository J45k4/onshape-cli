use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::types::{AssemblyResponse, Instance};

pub fn sanitize_name(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut prev_underscore = false;

    for ch in input.trim().chars() {
        let mapped = if ch.is_ascii_alphanumeric() || ch == '_' {
            ch
        } else {
            '_'
        };

        if mapped == '_' {
            if !prev_underscore {
                out.push('_');
                prev_underscore = true;
            }
        } else {
            out.push(mapped);
            prev_underscore = false;
        }
    }

    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "item".to_string()
    } else {
        trimmed.to_string()
    }
}

pub fn all_instances_from_asm(asm: &AssemblyResponse) -> Vec<Instance> {
    let mut out = Vec::new();
    if let Some(root) = &asm.root_assembly {
        out.extend(root.instances.clone());
    }
    for sub in &asm.sub_assemblies {
        out.extend(sub.instances.clone());
    }
    out
}

pub fn part_key(instance: &Instance) -> Option<String> {
    let did = instance
        .document_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let mv = instance
        .document_microversion
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let eid = instance
        .element_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let pid = instance
        .part_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    Some(format!("{did}:{mv}:{eid}:{pid}"))
}

pub fn write_json_pretty(path: &Path, value: &Value) -> Result<()> {
    let body = serde_json::to_string_pretty(value).context("failed to serialize JSON")?;
    fs::write(path, body).with_context(|| format!("failed to write {}", path.display()))
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let data = fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&data).with_context(|| format!("failed to parse JSON {}", path.display()))
}

pub fn append_jsonl(path: &Path, value: &Value) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;

    let mut obj = value.clone();
    if let Value::Object(map) = &mut obj {
        map.insert("t".to_string(), Value::String(Utc::now().to_rfc3339()));
    }

    let mut line = serde_json::to_string(&obj).context("failed to encode JSONL line")?;
    line.push('\n');

    file.write_all(line.as_bytes())
        .with_context(|| format!("failed to append {}", path.display()))
}

pub fn lower_eq(value: Option<&str>, target: &str) -> bool {
    value
        .map(|candidate| candidate.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}

pub fn unique_file_name(dir: &Path, base: &str, ext: &str) -> PathBuf {
    let ext = ext.trim_start_matches('.');
    let mut idx = 1usize;

    loop {
        let name = if idx == 1 {
            format!("{base}.{ext}")
        } else {
            format!("{base}_{idx}.{ext}")
        };

        let candidate = dir.join(name);
        if !candidate.exists() {
            return candidate;
        }
        idx += 1;
    }
}

pub fn path_to_unix(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::sanitize_name;

    #[test]
    fn sanitize_keeps_expected_format() {
        assert_eq!(sanitize_name("  Part 1  "), "Part_1");
        assert_eq!(sanitize_name("*&^"), "item");
    }
}
