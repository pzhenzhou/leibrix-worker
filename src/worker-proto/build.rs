use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Building Worker Proto...");

    let manifest_dir = env::var("CARGO_MANIFEST_DIR")?;
    let manifest_path = Path::new(&manifest_dir);
    let workspace_root: PathBuf = manifest_path
        .ancestors()
        .nth(2)
        .ok_or("Failed to find workspace root")?
        .to_path_buf();

    let proto_dir = workspace_root.join("proto");
    if !proto_dir.exists() {
        return Err(format!("Proto directory not found: {}", proto_dir.display()).into());
    }

    let common_proto = proto_dir.join("common.proto");
    let control_plane_proto = proto_dir.join("control_plane.proto");
    let ldp_proto = proto_dir.join("ldp.proto");

    if !common_proto.exists() {
        return Err(format!("Proto file not found: {}", common_proto.display()).into());
    }
    if !control_plane_proto.exists() {
        return Err(format!("Proto file not found: {}", control_plane_proto.display()).into());
    }
    if !ldp_proto.exists() {
        return Err(format!("Proto file not found: {}", ldp_proto.display()).into());
    }

    println!("cargo:rerun-if-changed={}", proto_dir.display());
    println!("cargo:rerun-if-changed={}", common_proto.display());
    println!("cargo:rerun-if-changed={}", control_plane_proto.display());
    println!("cargo:rerun-if-changed={}", ldp_proto.display());

    let out_dir = manifest_path.join("src").join("proto");
    fs::create_dir_all(&out_dir)?;
    println!("Output directory: {}", out_dir.display());

    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_well_known_types(true)
        .type_attribute(".", "#[allow(non_camel_case_types)]")
        .type_attribute(".", "#[allow(clippy::large_enum_variant)]")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .out_dir(&out_dir)
        .compile_protos(
            &[common_proto.clone(), control_plane_proto.clone(), ldp_proto.clone()],
            std::slice::from_ref(&proto_dir),
        )
        .map_err(|e| {
            format!(
                "Failed to compile protos: {}\n  Proto dir: {}\n  Common proto: {}\n  Control plane proto: {}\n  LDP proto: {}\n  Out dir: {}",
                e,
                proto_dir.display(),
                common_proto.display(),
                control_plane_proto.display(),
                ldp_proto.display(),
                out_dir.display()
            )
        })?;

    Ok(())
}
