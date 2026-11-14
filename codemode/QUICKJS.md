# Embedded QuickJS VM

`QuickJSSandbox` embeds two files from the npm packages Pi 1.5.2 installs, unmodified:

| File | From | License |
|---|---|---|
| `quickjs.wasm` | `quickjs-wasi` 3.6.2: QuickJS-NG v0.17.0 compiled to WebAssembly, with quickjs-wasi's C API | MIT: `LICENSE.quickjs-wasi` (Vercel, Inc.) and `LICENSE.quickjs-ng` (the QuickJS-NG authors) |
| `prelude.js` | `@earendil-works/pi-codemode` 1.5.2, `dist/runtime/prelude-source.js` (`PRELUDE_SOURCE`) | MIT, as the Pi monorepo (see `NOTICE`) |

SHA-256:

```
d4c9375f2b1ca4dc95f72c8aa2982a7a9951ac8011490d79c6582df732b4bbd9  quickjs.wasm
e64ad1a6afffbfb8f75a0d08434a9a3fec88d1e54206b03953bfc7b5820cc119  prelude.js
```

`quickjs_vm.go` and `quickjs_run.go` port quickjs-wasi's wrapper (`dist/index.js`, `dist/wasi-shim.js`) and pi-codemode's driver (`dist/runtime/vm.js`, `dist/runtime/execution.js`) to Go over wazero. A different module or prelude changes the VM's fingerprint, so scripts started under one do not resume under the other.
