#!/bin/sh
# Build an instrumented binary, run it on training data, merge the profile.
set -e
rustup component add llvm-tools >/dev/null 2>&1
PROF=$(find / -name llvm-profdata -type f 2>/dev/null | head -1)
cp -r /repo /work && cd /work
RUSTFLAGS="-C target-cpu=x86-64-v3 -Cprofile-generate=/tmp/pgo" cargo build --release --locked -q
for d in /dl /dm; do mkdir -p /tmp/out; DATA_DIR=$d OUTPUT_DIR=/tmp/out NCPU=8 ./target/release/sessions 2>&1 | tail -1; done
$PROF merge -o /pgoout/pgo.profdata /tmp/pgo
ls -la /pgoout/pgo.profdata
