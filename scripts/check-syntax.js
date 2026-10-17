import { readdir } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { resolve } from "node:path";

const files = (await readdir("resiliency"))
  .filter((name) => name.endsWith(".js"))
  .map((name) => resolve("resiliency", name));

for (const file of files) {
  const result = spawnSync(process.execPath, ["--check", file], { stdio: "inherit" });
  if (result.status !== 0) process.exit(result.status ?? 1);
}
