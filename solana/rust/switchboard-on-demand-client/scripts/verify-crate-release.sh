#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
crate_rel="rust/switchboard-on-demand-client"
crate_dir="$repo_root/$crate_rel"
manifest="$crate_dir/Cargo.toml"

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Refusing to package from a dirty git tree." >&2
  echo "Commit the release changes first, then rerun this script from main or a release tag." >&2
  exit 1
fi

head_sha="$(git rev-parse HEAD)"
branch="$(git rev-parse --abbrev-ref HEAD)"
tag_at_head="$(git tag --points-at HEAD | head -n 1)"

if [[ "$branch" != "main" && -z "$tag_at_head" ]]; then
  echo "Refusing to package from '$branch'." >&2
  echo "Publish from main or from a tag that points at the release commit." >&2
  exit 1
fi

if [[ "$branch" == "main" ]] && git rev-parse --verify origin/main >/dev/null 2>&1; then
  origin_main_sha="$(git rev-parse origin/main)"
  if [[ "$head_sha" != "$origin_main_sha" ]]; then
    echo "Refusing to package because local main does not match origin/main." >&2
    echo "HEAD:        $head_sha" >&2
    echo "origin/main: $origin_main_sha" >&2
    exit 1
  fi
fi

version="$(awk -F '"' '/^version = / { print $2; exit }' "$manifest")"
package_name="switchboard-on-demand-client-$version"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

CARGO_TARGET_DIR="$tmp_dir/target" cargo package --manifest-path "$manifest" --no-verify
crate_file="$tmp_dir/target/package/$package_name.crate"
vcs_info_file="$tmp_dir/.cargo_vcs_info.json"

tar -xOf "$crate_file" "$package_name/.cargo_vcs_info.json" > "$vcs_info_file"

node - "$vcs_info_file" "$head_sha" "$crate_rel" <<'NODE'
const fs = require('fs');

const [vcsInfoFile, expectedSha, expectedPath] = process.argv.slice(2);
const info = JSON.parse(fs.readFileSync(vcsInfoFile, 'utf8'));

if (!info.git) {
  throw new Error('missing git metadata in .cargo_vcs_info.json');
}

if (info.git.dirty !== false) {
  throw new Error('crate package was built from a dirty git state');
}

if (info.git.sha1 !== expectedSha) {
  throw new Error(
    `.cargo_vcs_info.json sha1 ${info.git.sha1} does not match HEAD ${expectedSha}`
  );
}

if (info.path_in_vcs !== expectedPath) {
  throw new Error(
    `.cargo_vcs_info.json path ${info.path_in_vcs} does not match ${expectedPath}`
  );
}
NODE

echo "Verified $crate_file"
echo ".cargo_vcs_info.json is clean and points at $head_sha"
