import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { isMain, parseArgs, readJson } from "./common.js";
import { AtlasClient, validateAtlasManifest } from "./atlas.js";

export async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv, { evidence: { type: "string" } });
  if (args._.length !== 1 || !args.evidence) throw new Error("Usage: cleanup-run.js <manifest> --evidence <path>");
  const manifestPath = resolve(args._[0]);
  const manifest = await readJson(manifestPath);
  validateAtlasManifest(manifest);
  const evidencePath = resolve(args.evidence);
  await mkdir(dirname(evidencePath), { recursive: true });
  await new AtlasClient().cleanup(manifest, manifestPath, evidencePath);
  console.log("Atlas cleanup verified");
  return 0;
}

if (isMain(import.meta.url)) process.exitCode = await main();
