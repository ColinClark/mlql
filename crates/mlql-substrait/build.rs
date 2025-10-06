// Build script to generate Substrait protobuf bindings

use std::io::Result;

fn main() -> Result<()> {
    // Configure prost-build
    let mut config = prost_build::Config::new();
    config
        .compile_well_known_types()
        .extern_path(".google.protobuf", "::prost_types")
        .extern_path(".google.protobuf.Empty", "()");

    // Use prost-reflect-build to also generate descriptor pool
    // This gives us JSON serialization via prost-reflect without needing serde
    let mut builder = prost_reflect_build::Builder::new();
    builder
        .descriptor_pool("crate::DESCRIPTOR_POOL")
        .compile_protos_with_config(
            config,
            &[
                "proto/substrait/extensions/extensions.proto",
                "proto/substrait/type.proto",
                "proto/substrait/type_expressions.proto",
                "proto/substrait/algebra.proto",
                "proto/substrait/plan.proto",
            ],
            &["proto/"],
        )?;

    println!("cargo:rerun-if-changed=proto/");
    Ok(())
}
