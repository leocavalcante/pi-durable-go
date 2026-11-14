#!/usr/bin/env bash
# Prepares upstream Pi Durable sources for the cross-checks (make crosscheck):
# a checkout of the pinned release, the dependencies of the installed Pi of
# the same version linked into it, a source shim for @earendil-works/chord,
# and copies of modules whose private functions the oracle needs exported.
# Prints the checkout path. Never part of make check.
set -euo pipefail

tag="${PI_DURABLE_UPSTREAM_TAG:-v1.5.2}"
version="${tag#v}"
work="${PI_DURABLE_CROSSCHECK_WORK:-$(pwd)/.crosscheck}"
upstream="${PI_DURABLE_UPSTREAM:-$work/pi-$version}"
modules="${PI_DURABLE_PI_MODULES:-$HOME/.pi/agent/install/releases/$version/node_modules}"

mkdir -p "$work"
if [ ! -d "$upstream/packages/durable/src" ]; then
	git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$tag" --filter=blob:none --sparse \
		https://github.com/earendil-works/pi "$upstream" >&2
	git -C "$upstream" sparse-checkout set packages/durable packages/ai packages/chord >&2
fi
if [ ! -d "$modules/@earendil-works/pi-ai" ]; then
	echo "crosscheck: no installed Pi $version dependencies at $modules (set PI_DURABLE_PI_MODULES)" >&2
	exit 1
fi
installed="$(node -p "require('$modules/@earendil-works/pi-ai/package.json').version")"
if [ "$installed" != "$version" ]; then
	echo "crosscheck: installed pi-ai is $installed, not $version" >&2
	exit 1
fi

# One node_modules for every upstream package: the installed Pi's, pi-ai's
# nested ones, and the chord shim.
deps="$work/node_modules-$version"
rm -rf "$deps"
mkdir -p "$deps/@earendil-works"
link() { [ -e "$2" ] || ln -s "$1" "$2"; }
for dir in "$modules"/* "$modules/@earendil-works/pi-ai/node_modules"/*; do
	[ -e "$dir" ] || continue
	name="$(basename "$dir")"
	case "$name" in
	@earendil-works) ;;
	@*)
		mkdir -p "$deps/$name"
		for scoped in "$dir"/*; do link "$scoped" "$deps/$name/$(basename "$scoped")"; done
		;;
	*) link "$dir" "$deps/$name" ;;
	esac
done
for dir in "$modules/@earendil-works"/*; do
	name="$(basename "$dir")"
	[ "$name" = chord ] || link "$dir" "$deps/@earendil-works/$name"
done

# Chord's dist is not built: its exports' source entries, as a package.
shim="$work/chord-$version"
rm -rf "$shim"
mkdir -p "$shim"
ln -s "$upstream/packages/chord/src" "$shim/src"
node -e '
const fs = require("fs");
const original = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
const exports = {};
for (const [key, value] of Object.entries(original.exports ?? {})) {
	exports[key] = value && typeof value === "object" && "source" in value ? value.source : value;
}
fs.writeFileSync(process.argv[2], JSON.stringify({ name: "@earendil-works/chord", type: "module", exports }, null, 1));
' "$upstream/packages/chord/package.json" "$shim/package.json"
ln -s "$shim" "$deps/@earendil-works/chord"

for package in durable ai chord; do
	target="$upstream/packages/$package/node_modules"
	if [ -L "$target" ] || [ ! -e "$target" ]; then
		rm -f "$target"
		ln -s "$deps" "$target"
	fi
done

# Private functions the oracle compares, exported from copies of their modules.
sed -e 's/^function planTools(/export function planTools(/' \
	-e 's/^function planSections(/export function planSections(/' \
	"$upstream/packages/durable/src/harness/prompt.ts" >"$upstream/packages/durable/src/harness/prompt.crosscheck.ts"
sed -e 's/^function checkClassifierContext(/export function checkClassifierContext(/' \
	-e 's/^function checkImagesContext(/export function checkImagesContext(/' \
	-e 's/^function resolveModel</export function resolveModel</' \
	"$upstream/packages/durable/src/codemode/models.ts" >"$upstream/packages/durable/src/codemode/models.crosscheck.ts"

echo "$upstream"
