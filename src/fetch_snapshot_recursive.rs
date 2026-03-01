use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    path::Path,
};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use reqwest::{
    Client, StatusCode, Url,
    header::{AUTHORIZATION, LOCATION},
    redirect::Policy,
};
use serde_json::{Value, json};

use crate::{
    cli::{FetchSnapshotRecursiveArgs, WvmKind},
    types::AssemblyResponse,
    util::{
        all_instances_from_asm, append_jsonl, lower_eq, part_key, path_to_unix, sanitize_name,
        unique_file_name, write_json_pretty,
    },
};

#[derive(Debug, Clone)]
struct AssemblyRef {
    did: String,
    mv: String,
    eid: String,
}

#[derive(Debug, Clone)]
enum QueueItem {
    Root {
        did: String,
        wvm: WvmKind,
        wvmid: String,
        eid: String,
    },
    Sub(AssemblyRef),
}

#[derive(Debug, Clone)]
struct ParsedOnshapeUrl {
    stack: String,
    did: String,
    wvm: WvmKind,
    wvmid: String,
    eid: String,
}

pub async fn run(args: FetchSnapshotRecursiveArgs) -> Result<()> {
    let access_key = args
        .access_key
        .context("missing access key: pass --access-key or set ONSHAPE_ACCESS_KEY")?;
    let secret_key = args
        .secret_key
        .context("missing secret key: pass --secret-key or set ONSHAPE_SECRET_KEY")?;

    let mut stack = args.stack;
    let mut did = args.did;
    let mut wvm = args.wvm;
    let mut wvmid = args.wvmid;
    let mut eid = args.eid;

    if let Some(raw_url) = args.url.as_deref() {
        let parsed = parse_onshape_url(raw_url)?;
        if using_default_stack(&stack) {
            stack = parsed.stack;
        }
        did = Some(parsed.did);
        wvm = Some(parsed.wvm);
        wvmid = Some(parsed.wvmid);
        eid = Some(parsed.eid);
    }

    let did = did.context("missing --did (or pass --url)")?;
    let source_wvm = wvm.context("missing --wvm (or pass --url)")?;
    let source_wvmid = wvmid.context("missing --wvmid (or pass --url)")?;
    let eid = eid.context("missing --eid (or pass --url)")?;
    let outdir = args.outdir.clone();

    let mut effective_wvm = source_wvm;
    let mut effective_wvmid = source_wvmid.clone();

    fs::create_dir_all(&outdir)
        .with_context(|| format!("failed to create {}", outdir.display()))?;

    let assemblies_dir = outdir.join("assemblies");
    fs::create_dir_all(&assemblies_dir)
        .with_context(|| format!("failed to create {}", assemblies_dir.display()))?;

    let mesh_dir_abs = outdir.join(&args.mesh_dir);
    if args.download_meshes {
        fs::create_dir_all(&mesh_dir_abs)
            .with_context(|| format!("failed to create {}", mesh_dir_abs.display()))?;
    }

    let api_base = format!("{}/api/{}", stack.trim_end_matches('/'), args.api_version);
    let auth_header = basic_auth_header(&access_key, &secret_key);

    let client = Client::builder().build().context("failed to build HTTP client")?;
    let no_redirect_client = Client::builder()
        .redirect(Policy::none())
        .build()
        .context("failed to build no-redirect HTTP client")?;

    let mut pinned_microversion: Option<String> = None;
    if args.pin_microversion && effective_wvm == WvmKind::W {
        let cmv_url = format!(
            "{api_base}/documents/d/{did}/w/{effective_wvmid}/currentmicroversion"
        );
        println!("Pinning microversion via: {cmv_url}");

        let cmv = fetch_json(&client, &cmv_url, &auth_header).await?;
        let microversion = cmv
            .get("microversionId")
            .or_else(|| cmv.get("id"))
            .or_else(|| cmv.get("microversion"))
            .and_then(Value::as_str)
            .context("could not read microversionId from currentmicroversion response")?
            .to_string();

        println!(
            "Pinned workspace w/{source_wvmid} -> microversion m/{microversion}"
        );

        effective_wvm = WvmKind::M;
        effective_wvmid = microversion.clone();
        pinned_microversion = Some(microversion);
    }

    let mut queue = VecDeque::new();
    queue.push_back(QueueItem::Root {
        did: did.clone(),
        wvm: effective_wvm,
        wvmid: effective_wvmid.clone(),
        eid: eid.clone(),
    });

    let mut seen_subassemblies: HashSet<String> = HashSet::new();
    let mut downloaded_part_keys: HashSet<String> = HashSet::new();
    let mut mesh_index: HashMap<String, String> = HashMap::new();
    let mut fetched_assemblies = 0usize;

    let errors_path = outdir.join("errors.jsonl");
    fs::write(&errors_path, "").with_context(|| format!("failed to initialize {}", errors_path.display()))?;

    while let Some(item) = queue.pop_front() {
        let result = handle_queue_item(
            &item,
            &client,
            &no_redirect_client,
            &auth_header,
            &api_base,
            &outdir,
            &assemblies_dir,
            &args.mesh_dir,
            &mesh_dir_abs,
            args.download_meshes,
            &errors_path,
            &mut seen_subassemblies,
            &mut downloaded_part_keys,
            &mut mesh_index,
            &mut fetched_assemblies,
            &mut queue,
        )
        .await;

        if let Err(error) = result {
            append_jsonl(
                &errors_path,
                &json!({
                    "kind": "assembly_fetch_failed",
                    "item": queue_item_json(&item),
                    "error": error.to_string(),
                }),
            )?;
        }
    }

    let manifest = json!({
        "createdAt": chrono::Utc::now().to_rfc3339(),
        "stack": stack,
        "apiVersion": args.api_version,
        "root": {
            "did": did,
            "sourceWvm": source_wvm.as_str(),
            "sourceWvmid": source_wvmid,
            "eid": eid,
        },
        "effectiveRoot": {
            "wvm": effective_wvm.as_str(),
            "wvmid": effective_wvmid,
            "pinnedMicroversion": pinned_microversion,
        },
        "fetchedAssemblies": fetched_assemblies,
        "fetchedSubassemblies": seen_subassemblies.len(),
        "downloadMeshes": args.download_meshes,
        "meshDir": path_to_unix(&args.mesh_dir),
        "meshesDownloaded": mesh_index.len(),
    });

    write_json_pretty(&outdir.join("manifest.json"), &manifest)?;
    write_json_pretty(
        &outdir.join("mesh_index.json"),
        &serde_json::to_value(&mesh_index)?,
    )?;

    println!();
    println!("Done.");
    println!("Snapshot: {}", outdir.display());
    println!("Assemblies saved: assembly_root.json + assemblies/*.json");
    if args.download_meshes {
        println!("Meshes: {} (count: {})", args.mesh_dir.display(), mesh_index.len());
    }
    println!("Errors (if any): errors.jsonl");

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_queue_item(
    item: &QueueItem,
    client: &Client,
    no_redirect_client: &Client,
    auth_header: &str,
    api_base: &str,
    outdir: &Path,
    assemblies_dir: &Path,
    mesh_dir_rel: &Path,
    mesh_dir_abs: &Path,
    download_meshes: bool,
    errors_path: &Path,
    seen_subassemblies: &mut HashSet<String>,
    downloaded_part_keys: &mut HashSet<String>,
    mesh_index: &mut HashMap<String, String>,
    fetched_assemblies: &mut usize,
    queue: &mut VecDeque<QueueItem>,
) -> Result<()> {
    let (asm_json, asm): (Value, AssemblyResponse) = match item {
        QueueItem::Root {
            did,
            wvm,
            wvmid,
            eid,
        } => {
            let mut url = Url::parse(&format!(
                "{api_base}/assemblies/d/{did}/{}/{}{}{}",
                wvm.as_str(),
                wvmid,
                "/e/",
                eid
            ))
            .context("failed to build root assembly URL")?;

            url.query_pairs_mut()
                .append_pair("includeMateFeatures", "true")
                .append_pair("includeMateConnectors", "true")
                .append_pair("includeNonSolids", "true");

            println!("Fetch ROOT assembly: {url}");
            let asm_json = fetch_json(client, url.as_str(), auth_header).await?;
            let asm: AssemblyResponse = serde_json::from_value(asm_json.clone())
                .context("failed to decode assembly response")?;

            write_json_pretty(&outdir.join("assembly_root.json"), &asm_json)?;
            (asm_json, asm)
        }
        QueueItem::Sub(reference) => {
            let key = asm_ref_key(reference);
            if !seen_subassemblies.insert(key) {
                return Ok(());
            }

            let mut url = Url::parse(&format!(
                "{api_base}/assemblies/d/{}/m/{}/e/{}",
                reference.did, reference.mv, reference.eid
            ))
            .context("failed to build subassembly URL")?;

            url.query_pairs_mut()
                .append_pair("includeMateFeatures", "true")
                .append_pair("includeMateConnectors", "true")
                .append_pair("includeNonSolids", "true");

            println!(
                "Fetch SUB assembly: {} m/{} e/{}",
                reference.did, reference.mv, reference.eid
            );

            let asm_json = fetch_json(client, url.as_str(), auth_header).await?;
            let asm: AssemblyResponse = serde_json::from_value(asm_json.clone())
                .context("failed to decode assembly response")?;

            let file_name = format!(
                "{}__{}__{}.json",
                sanitize_name(&reference.did),
                sanitize_name(&reference.mv),
                sanitize_name(&reference.eid)
            );
            write_json_pretty(&assemblies_dir.join(file_name), &asm_json)?;
            (asm_json, asm)
        }
    };

    let _ = asm_json;

    *fetched_assemblies += 1;
    let instances = all_instances_from_asm(&asm);

    for instance in &instances {
        if instance.suppressed.unwrap_or(false) {
            continue;
        }

        if lower_eq(instance.kind.as_deref(), "assembly") {
            let Some(sub_did) = instance.document_id.clone() else {
                continue;
            };
            let Some(sub_mv) = instance.document_microversion.clone() else {
                continue;
            };
            let Some(sub_eid) = instance.element_id.clone() else {
                continue;
            };

            queue.push_back(QueueItem::Sub(AssemblyRef {
                did: sub_did,
                mv: sub_mv,
                eid: sub_eid,
            }));
        }
    }

    if download_meshes {
        for instance in &instances {
            if instance.suppressed.unwrap_or(false) {
                continue;
            }
            if !lower_eq(instance.kind.as_deref(), "part") {
                continue;
            }

            let Some(pk) = part_key(instance) else {
                continue;
            };
            if !downloaded_part_keys.insert(pk.clone()) {
                continue;
            }

            let part_name = instance.name.as_deref().unwrap_or("part");
            let fallback = format!(
                "part_{}",
                instance.part_id.as_deref().unwrap_or("unknown_part")
            );
            let file_base = sanitize_name(instance.name.as_deref().unwrap_or(&fallback));
            let file_path = unique_file_name(mesh_dir_abs, &file_base, "stl");

            let Some(document_id) = instance.document_id.as_deref() else {
                continue;
            };
            let Some(document_microversion) = instance.document_microversion.as_deref() else {
                continue;
            };
            let Some(element_id) = instance.element_id.as_deref() else {
                continue;
            };
            let Some(part_id) = instance.part_id.as_deref() else {
                continue;
            };

            let mut stl_url = Url::parse(&format!(
                "{api_base}/parts/d/{document_id}/m/{document_microversion}/e/{element_id}/partid/{part_id}/stl"
            ))
            .context("failed to build STL URL")?;
            stl_url
                .query_pairs_mut()
                .append_pair("mode", "binary")
                .append_pair("units", "meter")
                .append_pair("scale", "1")
                .append_pair("grouping", "true");

            let file_name = file_path
                .file_name()
                .and_then(|value| value.to_str())
                .context("failed to derive STL filename")?
                .to_string();

            let relative_path = mesh_dir_rel.join(&file_name);

            match fetch_bytes_follow_redirect(
                no_redirect_client,
                client,
                stl_url.as_str(),
                auth_header,
            )
            .await
            {
                Ok(bytes) => {
                    fs::write(&file_path, bytes)
                        .with_context(|| format!("failed to write {}", file_path.display()))?;
                    mesh_index.insert(pk, path_to_unix(&relative_path));
                    println!("  STL: {part_name} -> {}", path_to_unix(&relative_path));
                }
                Err(error) => {
                    append_jsonl(
                        errors_path,
                        &json!({
                            "kind": "mesh_download_failed",
                            "partKey": pk,
                            "error": error.to_string(),
                        }),
                    )?;
                }
            }
        }
    }

    Ok(())
}

fn queue_item_json(item: &QueueItem) -> Value {
    match item {
        QueueItem::Root {
            did,
            wvm,
            wvmid,
            eid,
        } => json!({
            "kind": "root",
            "did": did,
            "wvm": wvm.as_str(),
            "wvmid": wvmid,
            "eid": eid,
        }),
        QueueItem::Sub(reference) => json!({
            "kind": "sub",
            "did": reference.did,
            "mv": reference.mv,
            "eid": reference.eid,
        }),
    }
}

fn asm_ref_key(reference: &AssemblyRef) -> String {
    format!("{}:{}:{}", reference.did, reference.mv, reference.eid)
}

fn using_default_stack(stack: &str) -> bool {
    stack == "https://cad.onshape.com"
}

fn basic_auth_header(access_key: &str, secret_key: &str) -> String {
    let token = BASE64_STANDARD.encode(format!("{access_key}:{secret_key}"));
    format!("Basic {token}")
}

async fn fetch_json(client: &Client, url: &str, auth_header: &str) -> Result<Value> {
    let response = client
        .get(url)
        .header(AUTHORIZATION, auth_header)
        .send()
        .await
        .with_context(|| format!("request failed: GET {url}"))?;

    let status = response.status();
    let body = response.text().await.context("failed to read response body")?;

    if !status.is_success() {
        bail!(format_http_error(status, &body));
    }

    serde_json::from_str(&body).context("failed to parse response JSON")
}

async fn fetch_bytes_follow_redirect(
    no_redirect_client: &Client,
    _client: &Client,
    url: &str,
    auth_header: &str,
) -> Result<Vec<u8>> {
    let original_url = Url::parse(url).context("invalid original URL")?;
    let mut current_url = original_url.clone();

    for _ in 0..10 {
        let mut request = no_redirect_client.get(current_url.clone());
        if should_forward_auth(&original_url, &current_url) {
            request = request.header(AUTHORIZATION, auth_header);
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("request failed: GET {current_url}"))?;

        let status = response.status();
        if status.is_redirection() {
            let location = response
                .headers()
                .get(LOCATION)
                .context("redirect without Location header")?
                .to_str()
                .context("invalid Location header")?;

            current_url = current_url
                .join(location)
                .or_else(|_| Url::parse(location))
                .context("invalid redirect URL")?;
            continue;
        }

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!(format_http_error(status, &body));
        }

        return Ok(response
            .bytes()
            .await
            .context("failed to read response bytes")?
            .to_vec());
    }

    bail!("too many redirects while downloading STL")
}

fn should_forward_auth(original: &Url, current: &Url) -> bool {
    if same_origin(original, current) {
        return true;
    }

    current.host_str().is_some_and(is_onshape_host)
}

fn same_origin(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme() && a.host_str() == b.host_str() && a.port_or_known_default() == b.port_or_known_default()
}

fn is_onshape_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("onshape.com")
        || host.eq_ignore_ascii_case("cad.onshape.com")
        || host.to_ascii_lowercase().ends_with(".onshape.com")
}

fn format_http_error(status: StatusCode, body: &str) -> String {
    format!(
        "HTTP {} {}\n{}",
        status.as_u16(),
        status.canonical_reason().unwrap_or("Unknown"),
        body
    )
}

fn parse_onshape_url(value: &str) -> Result<ParsedOnshapeUrl> {
    let url = Url::parse(value).with_context(|| format!("bad --url: {value}"))?;

    let host = url.host_str().context("URL has no host")?;
    let stack = if let Some(port) = url.port() {
        format!("{}://{}:{port}", url.scheme(), host)
    } else {
        format!("{}://{}", url.scheme(), host)
    };

    let segments: Vec<&str> = url.path().trim_matches('/').split('/').collect();
    let Some(documents_idx) = segments.iter().position(|segment| *segment == "documents") else {
        bail!("could not parse Onshape URL path: {}", url.path());
    };

    if segments.len() < documents_idx + 6 {
        bail!("could not parse Onshape URL path: {}", url.path());
    }

    let did = segments[documents_idx + 1].to_string();
    let wvm = match segments[documents_idx + 2] {
        "w" => WvmKind::W,
        "v" => WvmKind::V,
        "m" => WvmKind::M,
        other => bail!("unexpected version selector in URL: {other}"),
    };
    let wvmid = segments[documents_idx + 3].to_string();
    if segments[documents_idx + 4] != "e" {
        bail!("could not parse Onshape URL path: {}", url.path());
    }
    let eid = segments[documents_idx + 5].to_string();

    Ok(ParsedOnshapeUrl {
        stack,
        did,
        wvm,
        wvmid,
        eid,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_onshape_url;

    #[test]
    fn parse_url_extracts_components() {
        let parsed = parse_onshape_url("https://cad.onshape.com/documents/did123/w/wid456/e/eid789")
            .expect("URL should parse");

        assert_eq!(parsed.stack, "https://cad.onshape.com");
        assert_eq!(parsed.did, "did123");
        assert_eq!(parsed.wvmid, "wid456");
        assert_eq!(parsed.eid, "eid789");
    }
}
