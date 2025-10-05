/// Marks the crate as being compiled with nightly features enabled.
#[rustversion::nightly]
fn main() {
    println!("cargo::rustc-check-cfg=cfg(nightly)");
    println!("cargo:rustc-cfg=nightly");
}

/// Marks the crate as being compiled without nightly features.
#[rustversion::not(nightly)]
fn main() {
    println!("cargo::rustc-check-cfg=cfg(nightly)");
}
