fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("src/protobuf/tfplugin6.3.proto")?;
    tonic_build::compile_protos("src/protobuf/tfplugin5.3.proto")?;
    Ok(())
}
