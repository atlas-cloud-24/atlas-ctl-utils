#!/usr/bin/env bash
set -euo pipefail

run_local_step() {
  local github_workspace="${github_workspace:-/github/workspace}"
  local tooling_mode="${local_step_tooling_mode:-repo_url}"

  if [[ -z "${step_name:-}" ]]; then
    echo "❌ step_name must be set before calling run_local_step"
    exit 1
  fi
  if [[ -z "${step_dir:-}" ]]; then
    echo "❌ step_dir must be set before calling run_local_step"
    exit 1
  fi
  if [[ -z "${dockerfile_path:-}" ]]; then
    echo "❌ dockerfile_path must be set before calling run_local_step"
    exit 1
  fi

  : "${ATLAS_STEP_UTILS_DIR:?must be set}"
  local step_utils_dir_host
  step_utils_dir_host="$(realpath "$ATLAS_STEP_UTILS_DIR")"
  if [[ ! -d "$step_utils_dir_host/ctl" ]]; then
    echo "❌ ATLAS_STEP_UTILS_DIR/ctl not found: $step_utils_dir_host/ctl"
    exit 1
  fi

  if [[ "$tooling_mode" == "repo_path" ]]; then
    step_name="${step_name}-dev"
  elif [[ "$tooling_mode" != "repo_url" ]]; then
    echo "❌ unsupported local_step_tooling_mode: $tooling_mode"
    exit 1
  fi

  if ! declare -p local_step_extra_docker_args >/dev/null 2>&1; then
    declare -a local_step_extra_docker_args=()
  fi

  if declare -F local_step_before_build >/dev/null 2>&1; then
    local_step_before_build
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

  local -a step_cfg_mount_args=()
  if [[ -n "${TARGET_CFG_DIR:-}" ]]; then
    mkdir -p "${TARGET_CFG_DIR}"
    step_cfg_mount_args+=(
      -v "${TARGET_CFG_DIR}:/mnt/step_cfg"
      -e TARGET_CFG_DIR=/mnt/step_cfg
    )
  fi
  if [[ -n "${TARGET_ARTIFACTS_DIR:-}" ]]; then
    mkdir -p "${TARGET_ARTIFACTS_DIR}"
    step_cfg_mount_args+=(
      -v "${TARGET_ARTIFACTS_DIR}:/mnt/step_artifacts"
      -e TARGET_ARTIFACTS_DIR=/mnt/step_artifacts
    )
  fi

  docker build \
    -f "$dockerfile_path" \
    -t "$step_name" \
    "$step_dir"

  # Credential isolation: nothing from the host ~/.aws is ever mounted. Every
  # access path (role chain, direct, bypass) is resolved on the HOST to plain
  # session credentials and enters the box as env vars only.

  docker run --rm --name "$step_name" \
    --entrypoint sh \
    -v "$PWD:/mnt/source:ro" \
    -v "$(realpath "$origin_cfg_base_dir_path"):/mnt/origin_cfg:ro" \
    -v "$step_utils_dir_host:/mnt/step_utils:ro" \
    -e ATLAS_EXECUTION_CONTEXT_FILE \
    -e ATLAS_STEP_UTILS_DIR=/mnt/step_utils \
    -e step_dir="$step_dir" \
    -e cfg_files \
    -e STEP_WRITE_VALUES_JSON \
    -e STEP_WRITE_ENV_SH \
    -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}" \
    -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}" \
    -e AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
    -e ATLAS_AWS_ASSERT_ACCESS \
    -e ATLAS_AWS_PROFILE_ONLY_ACCESS \
    -e ATLAS_EXECUTION_IDENTITY_KEY \
    -e ATLAS_AWS_ACCOUNT_KEY \
    -e ATLAS_AWS_CREDENTIAL_SOURCE_KEY \
    -e ATLAS_AWS_IMPLEMENTATION_KEY \
    -e ATLAS_AWS_EXPECT_ACCOUNT_ID \
    -e ATLAS_AWS_EXPECT_PERMISSION_SET_NAME \
    -e ATLAS_AWS_EXPECT_ROLE_NAME \
    -e AWS_EC2_METADATA_DISABLED \
    -e GITHUB_WORKSPACE="$github_workspace" \
    "${tooling_args[@]}" \
    "${step_cfg_mount_args[@]}" \
    "${local_step_extra_docker_args[@]}" \
    "$step_name" -lc '
      set -e
      mkdir -p "$GITHUB_WORKSPACE"
      cp -a /mnt/source/. "$GITHUB_WORKSPACE"/
      cp -a /mnt/origin_cfg "$GITHUB_WORKSPACE"/origin_cfg
      cd "$GITHUB_WORKSPACE"

      if [ "${ATLAS_AWS_ASSERT_ACCESS}" = "true" ]; then
        python3 "${ATLAS_STEP_UTILS_DIR}/ctl/assert_aws_access.py"
      fi

      exec "./${step_dir}/src/step.sh"
    '
}
