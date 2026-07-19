#!/usr/bin/env bash
set -euo pipefail

# CTL-owned step runtime dispatcher (Phase 26). The engine invokes THIS — never
# a per-step run script. CTL owns the execution box; the step declares only its
# image + work in step.yaml (runtime.image / runtime.docker_build) and src/step.sh.
#
# Required env (set by the engine):
#   ATLAS_EXECUTION_RUNTIME_MODE       local | ci        — the active runtime (CTL selects)
#   ATLAS_STEP_NAME          unique box/tag name for this step
#   ATLAS_STEP_IMAGE         infra | ops        — step.yaml runtime.image
#   ATLAS_STEP_DOCKER_BUILD  true | false       — step.yaml runtime.docker_build
#   step_dir                 repo-relative step path (…/steps/<action>/<name>)
#   local_step_tooling_mode  repo_url | repo_path
#   ATLAS_STEP_UTILS_DIR     materialized ctl step-utils dir
# plus everything run_local_step already consumes (execution context, provider
# binding, cfg/artifact dirs, AWS_*/ATLAS_AWS_* passthroughs).

runtime="${ATLAS_EXECUTION_RUNTIME_MODE:?ATLAS_EXECUTION_RUNTIME_MODE must be set}"
: "${ATLAS_STEP_NAME:?ATLAS_STEP_NAME must be set}"
: "${ATLAS_STEP_IMAGE:?ATLAS_STEP_IMAGE must be set}"
: "${step_dir:?step_dir must be set}"
: "${ATLAS_STEP_UTILS_DIR:?must be set}"

case "$runtime" in
  local)
    # Local runtime: CTL builds+runs a fresh Docker box per step. Docker is how
    # the clean isolated box is produced locally; the same box comes free from a
    # GitHub Actions runner in the `ci` runtime.
    case "$ATLAS_STEP_IMAGE" in
      infra) dockerfile_path="${ATLAS_STEP_UTILS_DIR}/ctl/docker/Dockerfile.infra.local" ;;
      ops)   dockerfile_path="${ATLAS_STEP_UTILS_DIR}/ctl/docker/Dockerfile.ops.local" ;;
      *) echo "❌ unknown ATLAS_STEP_IMAGE: ${ATLAS_STEP_IMAGE}"; exit 1 ;;
    esac
    export dockerfile_path
    export step_name="$ATLAS_STEP_NAME"
    export local_step_tooling_mode="${local_step_tooling_mode:-repo_url}"

    if [ "${ATLAS_STEP_DOCKER_BUILD:-false}" = "true" ]; then
      # Steps that build/push container images need host Docker + multi-arch.
      declare -a local_step_extra_docker_args=(
        -v /var/run/docker.sock:/var/run/docker.sock
        -e AWS_PAGER=""
        -e BUILDKIT_PROGRESS=plain
      )
      local_step_before_build() {
        docker run --privileged --rm tonistiigi/binfmt --install all
      }
    fi

    source "${ATLAS_STEP_UTILS_DIR}/ctl/run_local_step.sh"
    run_local_step
    ;;
  ci)
    # CI runtime (not yet wired — Phase 26.3): default box = the SAME per-step
    # Docker image as local, run by CTL as the GHA job using the runner's host
    # Docker (NOT Docker-in-Docker) for byte-identical tooling. `ci` differs from
    # local in credentials (OIDC) and cfg source, not the box. A native-on-runner
    # mechanism (run src/step.sh directly) stays an optional per-step choice.
    echo "❌ ci runtime not yet implemented (Phase 26.3)"
    exit 1
    ;;
  *)
    echo "❌ unknown ATLAS_EXECUTION_RUNTIME_MODE: ${runtime}"
    exit 1
    ;;
esac
