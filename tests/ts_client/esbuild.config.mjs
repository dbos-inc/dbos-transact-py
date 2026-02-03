import esbuild from "esbuild";

await esbuild.build({
  entryPoints: ["portableinvoke.ts"],
  outfile: "dist/portableinvoke.cjs",
  bundle: true,
  platform: "node",
  target: "node20",
  format: "cjs",
  sourcemap: false,
  minify: true,
  treeShaking: true,
  // If you decide to use "pg" but not bundle it, you can mark it external:
  // external: ["pg"],
});

