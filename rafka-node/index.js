// Auto-selects the correct prebuilt binary for the current platform/arch.
// When installed from npm, the binary ships alongside this file.
// When developing locally, `npm run build:debug` produces the .node file.

const { platform, arch } = process;

const candidates = [
  // Platform-specific prebuilt (e.g. rafka-node.darwin-arm64.node)
  `./rafka-node.${platform}-${arch}.node`,
  // Debug build produced by `cargo build` / `napi build --platform`
  `./rafka-node.node`,
  // Cargo target directory (development fallback)
  `../target/debug/librafka_node.dylib`,
  `../target/debug/librafka_node.so`,
  `../target/debug/rafka_node.dll`,
];

let nativeBinding = null;
for (const candidate of candidates) {
  try {
    nativeBinding = require(candidate);
    break;
  } catch (_) {
    // try next
  }
}

if (!nativeBinding) {
  throw new Error(
    `Failed to load rafka native addon. ` +
      `Run \`npm run build\` in the rafka-node directory first.`,
  );
}

module.exports = nativeBinding;
