#!/usr/bin/env bash
set -euo pipefail

prepare_stage_runtime() {
    local stage_cfg_dir="$1"
    local cfg_files_json="${2:-["*"]}"

    rm -rf origin_cfg runtime bin lib
    mkdir -p origin_cfg
    cp -aL "$stage_cfg_dir"/. origin_cfg/
    export cfg_files="$cfg_files_json"

    source "${ATLAS_STAGE_UTILS_DIR:?must be set}/ctl/setup.sh"
}
