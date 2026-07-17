#!/usr/bin/env bash
set -euo pipefail

# CTL-owned stage runtime dispatcher (Phase 26). The engine invokes THIS — never
# a per-stage run script. CTL owns the execution box; the stage declares only its
# image + work in stage.yaml (runtime.image / runtime.docker_build) and src/stage.sh.
#
# Required env (set by the engine):
#   ATLAS_EXECUTION_RUNTIME_MODE       local | ci        — the active runtime (CTL selects)
#   ATLAS_STAGE_NAME          unique box/tag name for this stage
#   ATLAS_STAGE_IMAGE         infra | ops        — stage.yaml runtime.image
#   ATLAS_STAGE_DOCKER_BUILD  true | false       — stage.yaml runtime.docker_build
#   stage_dir                 repo-relative stage path (…/stages/<action>/<name>)
#   local_stage_tooling_mode  repo_url | repo_path
#   ATLAS_STAGE_UTILS_DIR     materialized ctl stage-utils dir
# plus everything run_local_stage already consumes (execution context, provider
# binding, cfg/artifact dirs, AWS_*/ATLAS_AWS_* passthroughs).

runtime="${ATLAS_EXECUTION_RUNTIME_MODE:?ATLAS_EXECUTION_RUNTIME_MODE must be set}"
: "${ATLAS_STAGE_NAME:?ATLAS_STAGE_NAME must be set}"
: "${ATLAS_STAGE_IMAGE:?ATLAS_STAGE_IMAGE must be set}"
: "${stage_dir:?stage_dir must be set}"
: "${ATLAS_STAGE_UTILS_DIR:?must be set}"

case "$runtime" in
  local)
    # Local runtime: CTL builds+runs a fresh Docker box per stage. Docker is how
    # the clean isolated box is produced locally; the same box comes free from a
    # GitHub Actions runner in the `ci` runtime.
    case "$ATLAS_STAGE_IMAGE" in
      infra) dockerfile_path="${ATLAS_STAGE_UTILS_DIR}/ctl/docker/Dockerfile.infra.local" ;;
      ops)   dockerfile_path="${ATLAS_STAGE_UTILS_DIR}/ctl/docker/Dockerfile.ops.local" ;;
      *) echo "❌ unknown ATLAS_STAGE_IMAGE: ${ATLAS_STAGE_IMAGE}"; exit 1 ;;
    esac
    export dockerfile_path
    export stage_name="$ATLAS_STAGE_NAME"
    export local_stage_tooling_mode="${local_stage_tooling_mode:-repo_url}"

    if [ "${ATLAS_STAGE_DOCKER_BUILD:-false}" = "true" ]; then
      # Stages that build/push container images need host Docker + multi-arch.
      declare -a local_stage_extra_docker_args=(
        -v /var/run/docker.sock:/var/run/docker.sock
        -e AWS_PAGER=""
        -e BUILDKIT_PROGRESS=plain
      )
      local_stage_before_build() {
        docker run --privileged --rm tonistiigi/binfmt --install all
      }
    fi

    source "${ATLAS_STAGE_UTILS_DIR}/ctl/run_local_stage.sh"
    run_local_stage
    ;;
  ci)
    # CI runtime (not yet wired — Phase 26.3): default box = the SAME per-stage
    # Docker image as local, run by CTL as the GHA job using the runner's host
    # Docker (NOT Docker-in-Docker) for byte-identical tooling. `ci` differs from
    # local in credentials (OIDC) and cfg source, not the box. A native-on-runner
    # mechanism (run src/stage.sh directly) stays an optional per-stage choice.
    echo "❌ ci runtime not yet implemented (Phase 26.3)"
    exit 1
    ;;
  *)
    echo "❌ unknown ATLAS_EXECUTION_RUNTIME_MODE: ${runtime}"
    exit 1
    ;;
esac
