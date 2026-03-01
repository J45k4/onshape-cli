use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

use crate::{
    cli::DumpToUrdfArgs,
    types::{AssemblyResponse, Instance, Occurrence},
    util::{all_instances_from_asm, part_key, read_json, sanitize_name},
};

#[derive(Debug, Clone)]
struct EmittedPart {
    link_name: String,
    joint_name: String,
    mesh_rel: String,
    xyz: [f64; 3],
    rpy: [f64; 3],
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
    let mut used_joint_names = HashSet::new();
    let mut emitted = Vec::new();

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
        let xyz = matrix_translation(&transform);
        let rotation = matrix_rotation3(&transform);
        let rpy = rpy_from_rotation(&rotation);

        let name_base = sanitize_name(
            instance
                .name
                .as_deref()
                .unwrap_or(instance.part_id.as_deref().unwrap_or("part")),
        );

        let link_name = unique_name(&name_base, &mut used_link_names);
        let joint_name = unique_name(&format!("{link_name}_joint"), &mut used_joint_names);

        emitted.push(EmittedPart {
            link_name,
            joint_name,
            mesh_rel,
            xyz,
            rpy,
        });
    }

    if emitted.is_empty() {
        bail!(
            "No part occurrences emitted. Check assembly_root.json and mesh_index.json (or fetch with --download-meshes)."
        );
    }

    let urdf = render_urdf(
        &robot_name,
        &mesh_prefix,
        mesh_scale,
        emit_collision,
        emit_visual,
        &emitted,
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

fn render_urdf(
    robot_name: &str,
    mesh_prefix: &str,
    mesh_scale: f64,
    emit_collision: bool,
    emit_visual: bool,
    parts: &[EmittedPart],
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

        lines.push(format!(
            "  <joint name=\"{}\" type=\"fixed\">",
            xml_escape(&part.joint_name)
        ));
        lines.push("    <parent link=\"base_link\"/>".to_string());
        lines.push(format!(
            "    <child link=\"{}\"/>",
            xml_escape(&part.link_name)
        ));
        lines.push(format!(
            "    <origin xyz=\"{} {} {}\" rpy=\"{} {} {}\"/>",
            fmt(part.xyz[0]),
            fmt(part.xyz[1]),
            fmt(part.xyz[2]),
            fmt(part.rpy[0]),
            fmt(part.rpy[1]),
            fmt(part.rpy[2])
        ));
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
    use super::{matrix_from_transform, matrix_rotation3, resolve_dump_paths, rpy_from_rotation};
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
}
