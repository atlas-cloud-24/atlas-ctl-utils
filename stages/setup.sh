#!/usr/bin/env bash
set -euo pipefail

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
for key in $(echo "$cfg_keys" | jq -r '.[]'); do
    echo "processing $key"
    if [ "$key" = "*" ]; then
        while IFS= read -r file; do
            echo "  merging: $file"
            cat "$file" >> .cfg
        done < <(find origin_cfg -type f)
        continue
    fi
    # if key ends with /* → merge directory contents recursively
    if [[ "$key" == *'/*' ]]; then
        dir="origin_cfg/${key%/*}"
        if [ -d "$dir" ]; then
            while IFS= read -r file; do
                echo "  merging: $file"
                cat "$file" >> .cfg
            done < <(find "$dir" -type f)
        else
            echo "skip: directory not found: $dir"
        fi
        continue
    fi
    # normal file merge (including paths like atlas/.common)
    file="origin_cfg/$key"
    if [ -f "$file" ]; then
        echo "  merging: $file"
        cat "$file" >> .cfg
    else
        echo "file not found: $file"
        exit 1
    fi
done
echo "------------------------------------------------------"

echo "=== 📦 Cloning plt_utils repo ==="
plt_utils_repo_url="https://github.com/atlas-cloud-24/atlas-plt-utils.git"
git clone --depth 1 "$plt_utils_repo_url" "$ext_dir_path/plt_utils"
plt_utils_repo_name="plt_utils"
plt_utils_repo_path="$ext_dir_path/$plt_utils_repo_name"
echo "plt_utils_repo_path=$plt_utils_repo_path"
echo "------------------------------------------------------"

echo "=== 🚚 Copying bin ==="
mkdir -p "$src_repo_path/bin"
cp -r "$plt_utils_repo_path/bin" "$src_repo_path"
echo "✅ bin copied"
echo "------------------------------------------------------"

echo "=== 📚 Copying lib ==="
mkdir -p "$src_repo_path/lib"
cp -r "$plt_utils_repo_path/lib" "$src_repo_path"
echo "✅ lib copied"
echo "------------------------------------------------------"

export src_repo_path=$src_repo_path

echo "✅ setup complete"
