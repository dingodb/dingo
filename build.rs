fn main() {
    let mut build = cxx_build::bridge("src/lib.rs");
    for flag in "-Wno-dollar-in-identifier-extension -Wno-unused-macros".split(' ') {
        build.flag(flag);
    }
    build.compile("tantivy_search");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=.cargo/config.toml");
}
