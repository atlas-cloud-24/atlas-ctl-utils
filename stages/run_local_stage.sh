#!/usr/bin/env bash
set -euo pipefail

run_local_stage() {
  local github_workspace="${github_workspace:-/github/workspace}"
  local tooling_mode="${local_stage_tooling_mode:-repo_url}"

  if [[ -z "${stage_name:-}" ]]; then
    echo "❌ stage_name must be set before calling run_local_stage"
    exit 1
  fi
  if [[ -z "${stage_dir:-}" ]]; then
    echo "❌ stage_dir must be set before calling run_local_stage"
    exit 1
  fi
  if [[ -z "${dockerfile_path:-}" ]]; then
    echo "❌ dockerfile_path must be set before calling run_local_stage"
    exit 1
  fi

  if [[ "$tooling_mode" == "repo_path" ]]; then
    stage_name="${stage_name}-dev"
  elif [[ "$tooling_mode" != "repo_url" ]]; then
    echo "❌ unsupported local_stage_tooling_mode: $tooling_mode"
    exit 1
  fi

  if ! declare -p local_stage_extra_docker_args >/dev/null 2>&1; then
    declare -a local_stage_extra_docker_args=()
  fi

  if declare -F local_stage_before_build >/dev/null 2>&1; then
    local_stage_before_build
  fi

  local -a tooling_args=()
  if [[ "$tooling_mode" == "repo_path" ]]; then
    if [[ -n "${ATLAS_CTL_UTILS_REPO_PATH:-}" ]]; then
      if [[ ! -d "${ATLAS_CTL_UTILS_REPO_PATH}" ]]; then
        echo "❌ ATLAS_CTL_UTILS_REPO_PATH not found: ${ATLAS_CTL_UTILS_REPO_PATH}"
        exit 1
      fi
      tooling_args+=(
        -v "${ATLAS_CTL_UTILS_REPO_PATH}:/mnt/local_tooling/atlas-ctl-utils:ro"
        -e ATLAS_CTL_UTILS_REPO_PATH=/mnt/local_tooling/atlas-ctl-utils
      )
    fi
    if [[ -n "${ATLAS_PLT_UTILS_REPO_PATH:-}" ]]; then
      if [[ ! -d "${ATLAS_PLT_UTILS_REPO_PATH}" ]]; then
        echo "❌ ATLAS_PLT_UTILS_REPO_PATH not found: ${ATLAS_PLT_UTILS_REPO_PATH}"
        exit 1
      fi
      tooling_args+=(
        -v "${ATLAS_PLT_UTILS_REPO_PATH}:/mnt/local_tooling/atlas-plt-utils:ro"
        -e ATLAS_PLT_UTILS_REPO_PATH=/mnt/local_tooling/atlas-plt-utils
      )
    fi
  else
    tooling_args+=(
      -e ATLAS_CTL_UTILS_REPO_URL
      -e ATLAS_CTL_UTILS_BRANCH
      -e ATLAS_CTL_UTILS_COMMIT
      -e ATLAS_PLT_UTILS_REPO_URL
      -e ATLAS_PLT_UTILS_BRANCH
      -e ATLAS_PLT_UTILS_COMMIT
    )
  fi

  local -a stage_cfg_mount_args=()
  if [[ -n "${STAGE_CFG_DIR:-}" ]]; then
    mkdir -p "${STAGE_CFG_DIR}"
    stage_cfg_mount_args+=(
      -v "${STAGE_CFG_DIR}:/mnt/stage_cfg"
      -e STAGE_CFG_DIR=/mnt/stage_cfg
    )
  fi

  docker build \
    -f "$dockerfile_path" \
    -t "$stage_name" \
    "$stage_dir"

  docker run --rm --name "$stage_name" \
    --entrypoint sh \
    -v "$PWD:/mnt/source:ro" \
    -v "$(realpath "$origin_cfg_base_dir_path"):/mnt/origin_cfg:ro" \
    -v "$HOME/.aws:/root/.aws:ro" \
    -e run_id \
    -e env_type \
    -e main_tag \
    -e stage_dir="$stage_dir" \
    -e cfg_files \
    -e STAGE_WRITE_VALUES_JSON \
    -e STAGE_WRITE_ENV_SH \
    -e AWS_PROFILE="$AWS_PROFILE" \
    -e GITHUB_WORKSPACE="$github_workspace" \
    "${tooling_args[@]}" \
    "${stage_cfg_mount_args[@]}" \
    "${local_stage_extra_docker_args[@]}" \
    "$stage_name" -lc '
      set -e
      mkdir -p "$GITHUB_WORKSPACE"
      cp -a /mnt/source/. "$GITHUB_WORKSPACE"/
      cp -a /mnt/origin_cfg "$GITHUB_WORKSPACE"/origin_cfg
      cd "$GITHUB_WORKSPACE"
      exec "./${stage_dir}/src/stage.sh"
    '
}
