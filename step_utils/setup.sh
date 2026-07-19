#!/usr/bin/env bash
set -euo pipefail

: "${ATLAS_STEP_UTILS_DIR:?must be set}"
if [[ ! -d "$ATLAS_STEP_UTILS_DIR" ]]; then
  echo "❌ ATLAS_STEP_UTILS_DIR not found: $ATLAS_STEP_UTILS_DIR"
  exit 1
fi

materialize_repo_ref() {
    local repo_path="${1:-}"
    local repo_url="${2:-}"
    local dest_path="$3"
    local branch="${4:-}"
    local commit="${5:-}"

    if [[ -n "$repo_path" && -n "$repo_url" ]]; then
        echo "❌ both repo_path and repo_url were provided"
        exit 1
    fi

    if [[ -n "$repo_path" ]]; then
        if [[ -n "$branch" || -n "$commit" ]]; then
            echo "❌ branch/commit are not supported with local repo_path: $repo_path"
            exit 1
        fi
        if [[ ! -d "$repo_path" ]]; then
            echo "❌ local repo_path not found: $repo_path"
            exit 1
        fi
        mkdir -p "$dest_path"
        cp -a "$repo_path"/. "$dest_path"/
        return
    fi

    if [[ -z "$repo_url" ]]; then
        echo "❌ repo_url or repo_path must be provided"
        exit 1
    fi

    if [[ -n "$branch" && -n "$commit" ]]; then
        echo "❌ both branch and commit were provided for $repo_url"
        exit 1
    fi

    if [[ -n "$commit" ]]; then
        git clone "$repo_url" "$dest_path"
        git -C "$dest_path" checkout "$commit"
        return
    fi

    if [[ -n "$branch" ]]; then
        git clone --branch "$branch" --depth 1 "$repo_url" "$dest_path"
        return
    fi

    git clone --depth 1 "$repo_url" "$dest_path"
}

echo "=== 🗂️ Get src repo name and dir ==="
src_repo_name=$(basename "$(git rev-parse --show-toplevel 2>/dev/null || pwd)")
src_repo_path=$GITHUB_WORKSPACE
src_repo_dir=$(dirname "$src_repo_path")
echo "src_repo_name=$src_repo_name"
echo "src_repo_path=$src_repo_path"
echo "src_repo_dir=$src_repo_dir"
echo "------------------------------------------------------"

echo "=== 🗂️ Creating external working directory ==="
ext_dir_path=$(mktemp -d /tmp/ext.XXXXXX)
echo "ext_dir_path=$ext_dir_path"
echo "------------------------------------------------------"

echo "=== 🗂️ Prepare cfg ==="
step_write_values_json="${STEP_WRITE_VALUES_JSON:-true}"
step_write_env_sh="${STEP_WRITE_ENV_SH:-true}"
values_json_out="-"
step_env_out="-"

if [[ "$step_write_values_json" == "true" || "$step_write_env_sh" == "true" ]]; then
  mkdir -p runtime
  : "${ATLAS_EXECUTION_CONTEXT_FILE:?must be set}"
  execution_context_file="$ATLAS_EXECUTION_CONTEXT_FILE"

  if [[ ! -f "$execution_context_file" ]]; then
    echo "❌ execution context file not found: $execution_context_file"
    exit 1
  fi

  if [[ "$step_write_values_json" == "true" ]]; then
    values_json_out="runtime/values.json"
  fi
  if [[ "$step_write_env_sh" == "true" ]]; then
    step_env_out="runtime/env.sh"
  fi

  python3 "${ATLAS_STEP_UTILS_DIR}/ctl/build_runtime_cfg.py" \
    --origin-cfg-dir origin_cfg \
    --cfg-files "$cfg_files" \
    --values-json-out "$values_json_out" \
    --step-env-out "$step_env_out" \
    --execution-context-file "$execution_context_file"

  echo "✅ cfg artifacts generated:"
  if [[ "$step_write_values_json" == "true" ]]; then
    echo "  - runtime/values.json"
  fi
  if [[ "$step_write_env_sh" == "true" ]]; then
    echo "  - runtime/env.sh"
  fi

  if [[ "$step_write_values_json" == "true" ]]; then
    export STEP_VALUES_JSON="$(realpath runtime/values.json)"
  else
    unset STEP_VALUES_JSON || true
  fi

  if [[ -n "${TARGET_CFG_DIR:-}" ]]; then
    # TARGET_CFG_DIR is the step's resolved/ layer: flat final consumables.
    mkdir -p "${TARGET_CFG_DIR}"
    rm -f "${TARGET_CFG_DIR}/values.json" "${TARGET_CFG_DIR}/env.sh"
    if [[ "$step_write_values_json" == "true" ]]; then
      cp runtime/values.json "${TARGET_CFG_DIR}/values.json"
    fi
    if [[ "$step_write_env_sh" == "true" ]]; then
      cp runtime/env.sh "${TARGET_CFG_DIR}/env.sh"
    fi
    chmod -R a+rwX "${TARGET_CFG_DIR}"
  fi
else
  unset STEP_VALUES_JSON || true
  echo "ℹ️ cfg runtime artifacts skipped"
fi
echo "------------------------------------------------------"

echo "=== 📦 Cloning plt_utils repo ==="
plt_utils_repo_path="${ATLAS_PLT_UTILS_REPO_PATH:-}"
plt_utils_repo_url="${ATLAS_PLT_UTILS_REPO_URL:-}"
plt_utils_branch="${ATLAS_PLT_UTILS_BRANCH:-}"
plt_utils_commit="${ATLAS_PLT_UTILS_COMMIT:-}"
materialize_repo_ref "$plt_utils_repo_path" "$plt_utils_repo_url" "$ext_dir_path/plt_utils" "$plt_utils_branch" "$plt_utils_commit"
plt_utils_repo_name="plt_utils"
plt_utils_repo_path="$ext_dir_path/$plt_utils_repo_name"
echo "plt_utils_repo_path=$plt_utils_repo_path"
echo "------------------------------------------------------"

echo "=== 🚚 Copying bin ==="
mkdir -p "$src_repo_path/bin"
cp -a "$plt_utils_repo_path/bin/." "$src_repo_path/bin/"
echo "✅ bin copied"
echo "------------------------------------------------------"

echo "=== 📚 Copying lib ==="
mkdir -p "$src_repo_path/lib"
cp -a "$plt_utils_repo_path/lib/." "$src_repo_path/lib/"
echo "✅ lib copied"
echo "------------------------------------------------------"

export src_repo_path=$src_repo_path

echo "✅ setup complete"
