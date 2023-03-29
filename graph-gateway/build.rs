use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(&["src/protobuf/kafka.proto"], &["src/protobuf"])?;
    Ok(())
}
