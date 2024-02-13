use std::env;
use std::path::PathBuf;
use std::process::Command;

/// Return the path to root of the crate being built.
///
/// The `CARGO_MANIFEST_DIR` env variable contains the path to the  directory containing the
/// manifest for the package being built (the package containing the build script). Also note that
/// this is the value of the current working directory of the build script when it starts.
///
/// https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-build-scripts
fn root_dir() -> PathBuf {
    PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
}

/// Check if all the build requirements are met.
///
/// This function checks if the following tools are installed:
/// - protoc (required by `prost-build`, see: https://github.com/tokio-rs/prost#protoc)
fn check_build_requirements() -> Result<(), String> {
    let mut errors = vec![];

    // Check if protoc is installed.
    let protoc = Command::new("protoc").arg("--version").status().unwrap();
    if !protoc.success() {
        errors.push(
            "protoc not found. Please install protoc: https://grpc.io/docs/protoc-installation/",
        );
    }

    if !errors.is_empty() {
        return Err(format!(
            "Build requirements not met:\n - {}",
            errors.join("\n - ")
        ));
    }

    Ok(())
}

fn main() {
    // Run code generation only if 'proto-gen' feature is enabled.
    if env::var("CARGO_FEATURE_PROTO_GEN").is_ok() {
        // Check if all the build requirements are met.
        if let Err(err) = check_build_requirements() {
            panic!("{}", err);
        }

        let src_dir = root_dir().join("src");
        let proto_dir = root_dir().join("proto");

        // Streamingfast Blockmeta service gRPC proto files
        let sf_blockmeta_proto_dir = proto_dir.join("sf/blockmeta/v2");
        let sf_blockmeta_src_dir = src_dir.join("chains/ethereum/sf_blockmeta_client");

        let status = tonic_build::configure()
            .build_client(true)
            .out_dir(sf_blockmeta_src_dir)
            .emit_rerun_if_changed(true)
            .compile(
                &[sf_blockmeta_proto_dir.join("blockmeta.proto")],
                &[sf_blockmeta_proto_dir],
            );

        if let Err(err) = status {
            panic!("Protobuf code generation failed: {}", err);
        }
    }
}
