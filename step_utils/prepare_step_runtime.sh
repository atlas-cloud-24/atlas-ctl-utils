#!/usr/bin/env bash
set -euo pipefail

prepare_step_runtime() {
    local step_cfg_dir="$1"
    local cfg_keys_json="${2:-}"
    if [[ -z "$cfg_keys_json" ]]; then
        cfg_keys_json="{}"
    fi

    rm -rf origin_cfg runtime bin lib
    mkdir -p origin_cfg
    cp -aL "$step_cfg_dir"/. origin_cfg/
    export cfg_keys="$cfg_keys_json"

    source "${ATLAS_STEP_UTILS_DIR:?must be set}/ctl/setup.sh"
}
