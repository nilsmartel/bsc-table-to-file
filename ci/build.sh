#/usr/bin/env sh

cargo build --release
cargo install --path . --force
