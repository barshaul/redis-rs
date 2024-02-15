/**
 * Copyright GLIDE-for-Redis Project Contributors - SPDX Identifier: Apache-2.0
 */
use const_gen::*;

fn main() {
    let out_dir = std::env::var_os("OUT_DIR").unwrap();
    let dest_path = std::path::Path::new(&out_dir).join("const_gen.rs");
    let version = std::env::var("GLIDE_VERSION").unwrap_or({
        let date = chrono::offset::Utc::now();
        date.format("dev-%Y-%m-%d-%H:%M").to_string()
    });
    let const_declarations = const_declaration!(GLIDE_VERSION = version);
    std::fs::write(&dest_path, const_declarations).unwrap();
}
