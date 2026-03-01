use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::{
    cli::DumpToUrdfArgs,
    types::{AssemblyResponse, Instance, Occurrence},
    util::{all_instances_from_asm, part_key, read_json, sanitize_name},
};

#[derive(Debug, Clone)]
struct EmittedPart {
    link_name: String,
    mesh_rel: String,
    world_transform: [f64; 16],
}

#[derive(Debug, Clone)]
struct EmittedJoint {
    joint_name: String,
    joint_type: &'static str,
    parent_link: String,
    child_link: String,
    xyz: [f64; 3],
    rpy: [f64; 3],
    axis: Option<[f64; 3]>,
}

pub fn run(args: DumpToUrdfArgs) -> Result<()> {
    let dump_input = args
        .dump
        .or(args.dump_positional)
        .unwrap_or_else(|| PathBuf::from("dump"));
    let (dump_dir, assembly_root_path) = resolve_dump_paths(&dump_input);

    let out_file = args.out.unwrap_or_else(|| dump_dir.join("robot.urdf"));
    let robot_name = sanitize_name(&args.robot);
    let mesh_prefix = args.mesh_prefix;
    let mesh_scale = args.mesh_scale;
    let emit_collision = !args.no_collision;
    let emit_visual = !args.no_visual;
    let skip_missing_mesh = args.skip_missing_mesh;

    let mesh_index_path = dump_dir.join("mesh_index.json");

    let root_asm: AssemblyResponse = read_json(&assembly_root_path)?;
    let mesh_index: HashMap<String, String> = if mesh_index_path.exists() {
        read_json(&mesh_index_path)?
    } else {
        HashMap::new()
    };

    let all_instances = collect_all_instances(&dump_dir, &root_asm)?;
    let mut instances_by_id: HashMap<String, Instance> = HashMap::new();

    for instance in all_instances {
        instances_by_id.entry(instance.id.clone()).or_insert(instance);
    }

    let occurrences = root_asm
        .root_assembly
        .as_ref()
        .map(|root| root.occurrences.clone())
        .unwrap_or_default();

    if occurrences.is_empty() {
        bail!(
            "No rootAssembly.occurrences found in {}",
            assembly_root_path.display()
        );
    }

    let mut used_link_names = HashSet::new();
    let mut emitted = Vec::new();
    let mut parts_by_occurrence: HashMap<String, usize> = HashMap::new();

    for occurrence in &occurrences {
        if should_skip_occurrence(occurrence) {
            continue;
        }
        let Some(tail_id) = occurrence.path.last() else {
            continue;
        };
        let Some(instance) = instances_by_id.get(tail_id) else {
            continue;
        };

        if instance.suppressed.unwrap_or(false) {
            continue;
        }
        if !instance
            .kind
            .as_deref()
            .map(|kind| kind.eq_ignore_ascii_case("part"))
            .unwrap_or(false)
        {
            continue;
        }

        let Some(pk) = part_key(instance) else {
            continue;
        };

        let Some(mesh_rel) = mesh_index.get(&pk).cloned() else {
            if skip_missing_mesh {
                continue;
            }
            bail!(
                "Missing mesh_index entry for partKey: {pk} (use --skip-missing-mesh true or regenerate meshes)"
            );
        };

        let transform = matrix_from_transform(&occurrence.transform);

        let name_base = sanitize_name(
            instance
                .name
                .as_deref()
                .unwrap_or(instance.part_id.as_deref().unwrap_or("part")),
        );

        let link_name = unique_name(&name_base, &mut used_link_names);
        let occurrence_key = occurrence_path_key(&occurrence.path);

        let part_idx = emitted.len();
        parts_by_occurrence.insert(occurrence_key.clone(), part_idx);

        emitted.push(EmittedPart {
            link_name,
            mesh_rel,
            world_transform: transform,
        });
    }

    if emitted.is_empty() {
        bail!(
            "No part occurrences emitted. Check assembly_root.json and mesh_index.json (or fetch with --download-meshes)."
        );
    }

    let features = root_asm
        .root_assembly
        .as_ref()
        .map(|root| root.features.as_slice())
        .unwrap_or(&[]);

    let mut used_joint_names = HashSet::new();
    let joints = build_joints(
        &emitted,
        &parts_by_occurrence,
        features,
        &mut used_joint_names,
    );

    let urdf = render_urdf(
        &robot_name,
        &mesh_prefix,
        mesh_scale,
        emit_collision,
        emit_visual,
        &emitted,
        &joints,
    );

    if let Some(parent) = out_file.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    fs::write(&out_file, urdf).with_context(|| format!("failed to write {}", out_file.display()))?;

    println!("Wrote URDF: {}", out_file.display());
    println!("Links: {} (including base_link)", emitted.len() + 1);
    println!(
        "Tip: mesh paths in URDF are \"{}<mesh_index.json relpaths>\"",
        mesh_prefix
    );

    Ok(())
}

fn resolve_dump_paths(input: &Path) -> (PathBuf, PathBuf) {
    let treat_as_json_file = input
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("json"))
        .unwrap_or(false);

    if treat_as_json_file {
        let dump_dir = input
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        (dump_dir, input.to_path_buf())
    } else {
        (input.to_path_buf(), input.join("assembly_root.json"))
    }
}

fn collect_all_instances(dump_dir: &Path, root_asm: &AssemblyResponse) -> Result<Vec<Instance>> {
    let mut out = all_instances_from_asm(root_asm);

    let assemblies_dir = dump_dir.join("assemblies");
    if !assemblies_dir.exists() {
        return Ok(out);
    }

    for entry in fs::read_dir(&assemblies_dir)
        .with_context(|| format!("failed to read {}", assemblies_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        if let Ok(assembly) = read_json::<AssemblyResponse>(&path) {
            out.extend(all_instances_from_asm(&assembly));
        }
    }

    Ok(out)
}

fn should_skip_occurrence(occurrence: &Occurrence) -> bool {
    occurrence.suppressed.unwrap_or(false)
        || occurrence.is_suppressed.unwrap_or(false)
        || occurrence.hidden.unwrap_or(false)
        || occurrence.is_hidden.unwrap_or(false)
}

fn unique_name(base: &str, used: &mut HashSet<String>) -> String {
    let mut idx = 1usize;

    loop {
        let candidate = if idx == 1 {
            base.to_string()
        } else {
            format!("{base}_{idx}")
        };

        if used.insert(candidate.clone()) {
            return candidate;
        }

        idx += 1;
    }
}

fn matrix_from_transform(transform: &[f64]) -> [f64; 16] {
    if transform.len() != 16 {
        return [
            1.0, 0.0, 0.0, 0.0, //
            0.0, 1.0, 0.0, 0.0, //
            0.0, 0.0, 1.0, 0.0, //
            0.0, 0.0, 0.0, 1.0,
        ];
    }

    let mut matrix = [0.0f64; 16];
    matrix.copy_from_slice(transform);
    matrix
}

fn matrix_translation(matrix: &[f64; 16]) -> [f64; 3] {
    [matrix[3], matrix[7], matrix[11]]
}

fn matrix_rotation3(matrix: &[f64; 16]) -> [f64; 9] {
    [
        matrix[0], matrix[1], matrix[2], matrix[4], matrix[5], matrix[6], matrix[8], matrix[9],
        matrix[10],
    ]
}

fn rpy_from_rotation(rotation: &[f64; 9]) -> [f64; 3] {
    let r11 = rotation[0];
    let r21 = rotation[3];
    let r23 = rotation[5];
    let r31 = rotation[6];
    let r32 = rotation[7];
    let r33 = rotation[8];

    let sy = (r11 * r11 + r21 * r21).sqrt();
    let singular = sy < 1e-9;

    if !singular {
        [r32.atan2(r33), (-r31).atan2(sy), r21.atan2(r11)]
    } else {
        [(-r23).atan2(rotation[4]), (-r31).atan2(sy), 0.0]
    }
}

fn occurrence_path_key(path: &[String]) -> String {
    path.join("/")
}

fn build_joints(
    parts: &[EmittedPart],
    parts_by_occurrence: &HashMap<String, usize>,
    features: &[Value],
    used_joint_names: &mut HashSet<String>,
) -> Vec<EmittedJoint> {
    let mut joints = Vec::new();
    let mut parent_of: Vec<Option<usize>> = vec![None; parts.len()];

    for feature in features {
        if feature.get("featureType").and_then(Value::as_str) != Some("mate") {
            continue;
        }
        if feature
            .get("suppressed")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            continue;
        }

        let Some(feature_data) = feature.get("featureData") else {
            continue;
        };
        let Some(mate_type_raw) = feature_data.get("mateType").and_then(Value::as_str) else {
            continue;
        };
        let Some(joint_type) = map_mate_type(mate_type_raw) else {
            continue;
        };

        let Some(mated_entities) = feature_data.get("matedEntities").and_then(Value::as_array)
        else {
            continue;
        };
        if mated_entities.len() != 2 {
            continue;
        }

        let Some(path_a) = mated_entities[0]
            .get("matedOccurrence")
            .and_then(parse_occurrence_path)
        else {
            continue;
        };
        let Some(path_b) = mated_entities[1]
            .get("matedOccurrence")
            .and_then(parse_occurrence_path)
        else {
            continue;
        };

        let key_a = occurrence_path_key(&path_a);
        let key_b = occurrence_path_key(&path_b);

        let Some(&part_a_idx) = parts_by_occurrence.get(&key_a) else {
            continue;
        };
        let Some(&part_b_idx) = parts_by_occurrence.get(&key_b) else {
            continue;
        };

        if part_a_idx == part_b_idx {
            continue;
        }

        let (parent_idx, child_idx, child_entity) = if parent_of[part_b_idx].is_none() {
            (part_a_idx, part_b_idx, &mated_entities[1])
        } else if parent_of[part_a_idx].is_none() {
            (part_b_idx, part_a_idx, &mated_entities[0])
        } else {
            continue;
        };

        if creates_cycle(parent_idx, child_idx, &parent_of) {
            continue;
        }

        let parent_world = parts[parent_idx].world_transform;
        let child_world = parts[child_idx].world_transform;
        let parent_to_child = matrix_multiply(&matrix_inverse_rigid(&parent_world), &child_world);

        let xyz = matrix_translation(&parent_to_child);
        let rotation = matrix_rotation3(&parent_to_child);
        let rpy = rpy_from_rotation(&rotation);

        let axis = match joint_type {
            "revolute" | "prismatic" | "continuous" => {
                axis_from_mated_entity(child_entity).or(Some([0.0, 0.0, 1.0]))
            }
            _ => None,
        };

        let joint_base = feature_data
            .get("name")
            .and_then(Value::as_str)
            .map(sanitize_name)
            .unwrap_or_else(|| "joint".to_string());
        let joint_name = unique_name(&joint_base, used_joint_names);

        joints.push(EmittedJoint {
            joint_name,
            joint_type,
            parent_link: parts[parent_idx].link_name.clone(),
            child_link: parts[child_idx].link_name.clone(),
            xyz,
            rpy,
            axis,
        });

        parent_of[child_idx] = Some(parent_idx);
    }

    for (idx, part) in parts.iter().enumerate() {
        if parent_of[idx].is_some() {
            continue;
        }

        let xyz = matrix_translation(&part.world_transform);
        let rotation = matrix_rotation3(&part.world_transform);
        let rpy = rpy_from_rotation(&rotation);
        let joint_name = unique_name(&format!("{}_joint", part.link_name), used_joint_names);

        joints.push(EmittedJoint {
            joint_name,
            joint_type: "fixed",
            parent_link: "base_link".to_string(),
            child_link: part.link_name.clone(),
            xyz,
            rpy,
            axis: None,
        });
    }

    joints
}

fn creates_cycle(parent_idx: usize, child_idx: usize, parent_of: &[Option<usize>]) -> bool {
    let mut current = Some(parent_idx);
    while let Some(idx) = current {
        if idx == child_idx {
            return true;
        }
        current = parent_of[idx];
    }
    false
}

fn map_mate_type(value: &str) -> Option<&'static str> {
    match value {
        "FASTENED" => Some("fixed"),
        "REVOLUTE" => Some("revolute"),
        "SLIDER" => Some("prismatic"),
        "CYLINDRICAL" => Some("continuous"),
        _ => None,
    }
}

fn parse_occurrence_path(value: &Value) -> Option<Vec<String>> {
    let items = value.as_array()?;
    if items.is_empty() {
        return None;
    }

    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(item.as_str()?.to_string());
    }
    Some(out)
}

fn axis_from_mated_entity(entity: &Value) -> Option<[f64; 3]> {
    let mated_cs = entity.get("matedCS")?;
    let mut axis = vec3_from_value(mated_cs.get("zAxis")?)?;
    let norm = (axis[0] * axis[0] + axis[1] * axis[1] + axis[2] * axis[2]).sqrt();

    if norm < 1e-12 {
        return None;
    }

    axis[0] /= norm;
    axis[1] /= norm;
    axis[2] /= norm;
    Some(axis)
}

fn vec3_from_value(value: &Value) -> Option<[f64; 3]> {
    let items = value.as_array()?;
    if items.len() != 3 {
        return None;
    }

    Some([
        items[0].as_f64()?,
        items[1].as_f64()?,
        items[2].as_f64()?,
    ])
}

fn matrix_inverse_rigid(matrix: &[f64; 16]) -> [f64; 16] {
    let r00 = matrix[0];
    let r01 = matrix[1];
    let r02 = matrix[2];
    let tx = matrix[3];

    let r10 = matrix[4];
    let r11 = matrix[5];
    let r12 = matrix[6];
    let ty = matrix[7];

    let r20 = matrix[8];
    let r21 = matrix[9];
    let r22 = matrix[10];
    let tz = matrix[11];

    let ir00 = r00;
    let ir01 = r10;
    let ir02 = r20;

    let ir10 = r01;
    let ir11 = r11;
    let ir12 = r21;

    let ir20 = r02;
    let ir21 = r12;
    let ir22 = r22;

    let itx = -(ir00 * tx + ir01 * ty + ir02 * tz);
    let ity = -(ir10 * tx + ir11 * ty + ir12 * tz);
    let itz = -(ir20 * tx + ir21 * ty + ir22 * tz);

    [
        ir00, ir01, ir02, itx, //
        ir10, ir11, ir12, ity, //
        ir20, ir21, ir22, itz, //
        0.0, 0.0, 0.0, 1.0,
    ]
}

fn matrix_multiply(left: &[f64; 16], right: &[f64; 16]) -> [f64; 16] {
    let mut out = [0.0f64; 16];

    for row in 0..4 {
        for col in 0..4 {
            out[row * 4 + col] = left[row * 4] * right[col]
                + left[row * 4 + 1] * right[4 + col]
                + left[row * 4 + 2] * right[8 + col]
                + left[row * 4 + 3] * right[12 + col];
        }
    }

    out
}

fn render_urdf(
    robot_name: &str,
    mesh_prefix: &str,
    mesh_scale: f64,
    emit_collision: bool,
    emit_visual: bool,
    parts: &[EmittedPart],
    joints: &[EmittedJoint],
) -> String {
    let scale_attr = if mesh_scale.is_finite() && (mesh_scale - 1.0).abs() > f64::EPSILON {
        format!(
            " scale=\"{} {} {}\"",
            fmt(mesh_scale),
            fmt(mesh_scale),
            fmt(mesh_scale)
        )
    } else {
        String::new()
    };

    let mut lines = vec![
        "<?xml version=\"1.0\"?>".to_string(),
        format!("<robot name=\"{}\">", xml_escape(robot_name)),
        "  <link name=\"base_link\"/>".to_string(),
    ];

    for part in parts {
        let mesh_file = format!("{mesh_prefix}{}", part.mesh_rel);

        lines.push(format!("  <link name=\"{}\">", xml_escape(&part.link_name)));

        if emit_visual {
            lines.push("    <visual>".to_string());
            lines.push("      <origin xyz=\"0 0 0\" rpy=\"0 0 0\"/>".to_string());
            lines.push("      <geometry>".to_string());
            lines.push(format!(
                "        <mesh filename=\"{}\"{scale_attr}/>",
                xml_escape(&mesh_file)
            ));
            lines.push("      </geometry>".to_string());
            lines.push("    </visual>".to_string());
        }

        if emit_collision {
            lines.push("    <collision>".to_string());
            lines.push("      <origin xyz=\"0 0 0\" rpy=\"0 0 0\"/>".to_string());
            lines.push("      <geometry>".to_string());
            lines.push(format!(
                "        <mesh filename=\"{}\"{scale_attr}/>",
                xml_escape(&mesh_file)
            ));
            lines.push("      </geometry>".to_string());
            lines.push("    </collision>".to_string());
        }

        lines.push("  </link>".to_string());
    }

    for joint in joints {
        lines.push(format!(
            "  <joint name=\"{}\" type=\"{}\">",
            xml_escape(&joint.joint_name),
            joint.joint_type
        ));
        lines.push(format!(
            "    <parent link=\"{}\"/>",
            xml_escape(&joint.parent_link)
        ));
        lines.push(format!(
            "    <child link=\"{}\"/>",
            xml_escape(&joint.child_link)
        ));
        lines.push(format!(
            "    <origin xyz=\"{} {} {}\" rpy=\"{} {} {}\"/>",
            fmt(joint.xyz[0]),
            fmt(joint.xyz[1]),
            fmt(joint.xyz[2]),
            fmt(joint.rpy[0]),
            fmt(joint.rpy[1]),
            fmt(joint.rpy[2])
        ));

        if let Some(axis) = joint.axis {
            lines.push(format!(
                "    <axis xyz=\"{} {} {}\"/>",
                fmt(axis[0]),
                fmt(axis[1]),
                fmt(axis[2])
            ));
        }

        lines.push("  </joint>".to_string());
    }

    lines.push("</robot>".to_string());
    lines.join("\n")
}

fn fmt(mut value: f64) -> String {
    if !value.is_finite() {
        value = 0.0;
    }
    if value.abs() < 1e-12 {
        value = 0.0;
    }

    let mut s = format!("{value:.6}");
    while s.contains('.') && s.ends_with('0') {
        s.pop();
    }
    if s.ends_with('.') {
        s.pop();
    }
    if s == "-0" {
        "0".to_string()
    } else {
        s
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::{
        EmittedPart, build_joints, matrix_from_transform, matrix_rotation3, resolve_dump_paths,
        rpy_from_rotation,
    };
    use serde_json::json;
    use std::collections::{HashMap, HashSet};
    use std::path::Path;

    #[test]
    fn identity_rotation_has_zero_rpy() {
        let matrix = matrix_from_transform(&[
            1.0, 0.0, 0.0, 0.0, //
            0.0, 1.0, 0.0, 0.0, //
            0.0, 0.0, 1.0, 0.0, //
            0.0, 0.0, 0.0, 1.0,
        ]);
        let rotation = matrix_rotation3(&matrix);
        let rpy = rpy_from_rotation(&rotation);

        assert!(rpy[0].abs() < 1e-12);
        assert!(rpy[1].abs() < 1e-12);
        assert!(rpy[2].abs() < 1e-12);
    }

    #[test]
    fn resolve_dump_paths_for_directory_input() {
        let (dump_dir, assembly_root) = resolve_dump_paths(Path::new("dump"));
        assert_eq!(dump_dir, Path::new("dump"));
        assert_eq!(assembly_root, Path::new("dump/assembly_root.json"));
    }

    #[test]
    fn resolve_dump_paths_for_json_input() {
        let (dump_dir, assembly_root) =
            resolve_dump_paths(Path::new("./workdir/puppyarm/assembly_root.json"));
        assert_eq!(dump_dir, Path::new("./workdir/puppyarm"));
        assert_eq!(assembly_root, Path::new("./workdir/puppyarm/assembly_root.json"));
    }

    #[test]
    fn build_joints_creates_mate_and_root_joint() {
        let parts = vec![
            EmittedPart {
                link_name: "parent".to_string(),
                mesh_rel: "meshes/parent.stl".to_string(),
                world_transform: [
                    1.0, 0.0, 0.0, 0.0, //
                    0.0, 1.0, 0.0, 0.0, //
                    0.0, 0.0, 1.0, 0.0, //
                    0.0, 0.0, 0.0, 1.0,
                ],
            },
            EmittedPart {
                link_name: "child".to_string(),
                mesh_rel: "meshes/child.stl".to_string(),
                world_transform: [
                    1.0, 0.0, 0.0, 1.0, //
                    0.0, 1.0, 0.0, 0.0, //
                    0.0, 0.0, 1.0, 0.0, //
                    0.0, 0.0, 0.0, 1.0,
                ],
            },
        ];

        let mut by_occurrence = HashMap::new();
        by_occurrence.insert("A".to_string(), 0usize);
        by_occurrence.insert("B".to_string(), 1usize);

        let features = vec![json!({
            "featureType": "mate",
            "suppressed": false,
            "featureData": {
                "name": "joint_ab",
                "mateType": "REVOLUTE",
                "matedEntities": [
                    {
                        "matedOccurrence": ["A"],
                        "matedCS": { "zAxis": [0.0, 0.0, 1.0] }
                    },
                    {
                        "matedOccurrence": ["B"],
                        "matedCS": { "zAxis": [0.0, 1.0, 0.0] }
                    }
                ]
            }
        })];

        let mut used_joint_names = HashSet::new();
        let joints = build_joints(&parts, &by_occurrence, &features, &mut used_joint_names);

        assert_eq!(joints.len(), 2);

        let mate_joint = joints
            .iter()
            .find(|joint| joint.joint_name == "joint_ab")
            .expect("mate joint should exist");
        assert_eq!(mate_joint.joint_type, "revolute");
        assert_eq!(mate_joint.parent_link, "parent");
        assert_eq!(mate_joint.child_link, "child");
        assert!((mate_joint.xyz[0] - 1.0).abs() < 1e-12);
        assert!(mate_joint.xyz[1].abs() < 1e-12);
        assert!(mate_joint.xyz[2].abs() < 1e-12);
        assert_eq!(mate_joint.axis, Some([0.0, 1.0, 0.0]));

        let root_joint = joints
            .iter()
            .find(|joint| joint.parent_link == "base_link")
            .expect("root part should be attached to base");
        assert_eq!(root_joint.child_link, "parent");
        assert_eq!(root_joint.joint_type, "fixed");
    }
}
