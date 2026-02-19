import esbuild from "esbuild";

await esbuild.build({
  entryPoints: ["portableinvoke.ts"],
  outfile: "bundles/portableinvoke.cjs",
  bundle: true,
  platform: "node",
  target: "node20",
  format: "cjs",
  sourcemap: false,
  minify: true,
  treeShaking: true,
  // Optional telemetry / logging deps that the client path never touches:
  external: [
    "winston", "winston-transport",
    "@opentelemetry/*",
  ],
});

