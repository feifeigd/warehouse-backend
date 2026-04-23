#!/usr/bin/env bash
set -euo pipefail

PRESET="${CMAKE_PRESET:-linux-x64-debug}"

require_tool() {
  local tool_name="$1"

  if ! command -v "${tool_name}" >/dev/null 2>&1; then
    echo "Missing required tool: ${tool_name}" >&2
    echo "Rebuild the devcontainer so the Dockerfile provisions the full Linux toolchain." >&2
    exit 1
  fi
}

require_tool gcc
require_tool g++
require_tool ninja

echo "C++ toolchain:"
g++ --version | head -n 1
cmake --version | head -n 1
echo ninja version:
ninja --version
echo cmake preset:
echo "${PRESET}"

if [ -f CMakeLists.txt ]; then
  # 生成
  # cmake --fresh --preset "${PRESET}"
  cmake --preset "${PRESET}"
  # 构建
  cmake --build --preset "${PRESET}"
else
  echo "No CMakeLists.txt found yet. The devcontainer is ready for C++ development."
fi
