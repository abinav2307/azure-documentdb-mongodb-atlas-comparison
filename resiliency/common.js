import { randomBytes } from "node:crypto";
import { readFile, rename, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export function utcNow() {
  return new Date().toISOString();
}

export function round(value, digits = 3) {
  const factor = 10 ** digits;
  return Math.round((value + Number.EPSILON) * factor) / factor;
}

export function sleep(milliseconds) {
  return new Promise((resolvePromise) => setTimeout(resolvePromise, milliseconds));
}

export function readSecret(prompt) {
  if (!process.stdin.isTTY || !process.stdout.isTTY || typeof process.stdin.setRawMode !== "function") {
    throw new Error("Masked input requires an interactive terminal; set the environment variable instead");
  }
  return new Promise((resolvePromise, reject) => {
    let value = "";
    const previousRaw = process.stdin.isRaw;
    process.stdout.write(prompt);
    process.stdin.setRawMode(true);
    process.stdin.resume();
    process.stdin.setEncoding("utf8");
    const cleanup = () => {
      process.stdin.off("data", onData);
      process.stdin.setRawMode(Boolean(previousRaw));
      process.stdout.write("\n");
    };
    const onData = (chunk) => {
      for (const character of chunk) {
        if (character === "\r" || character === "\n") {
          cleanup();
          resolvePromise(value);
          return;
        }
        if (character === "\u0003") {
          cleanup();
          reject(new Error("Secret input cancelled"));
          return;
        }
        if (character === "\u007f" || character === "\b") value = value.slice(0, -1);
        else if (character >= " ") value += character;
      }
    };
    process.stdin.on("data", onData);
  });
}

export async function readJson(path) {
  return JSON.parse(await readFile(path, "utf8"));
}

export async function writeJson(path, value) {
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

export async function atomicJson(path, value) {
  const temporary = `${path}.tmp`;
  await writeJson(temporary, value);
  await replaceWithRetry(temporary, path);
}

export async function atomicText(path, value) {
  const temporary = `${path}.tmp`;
  await writeFile(temporary, `${value}\n`, "utf8");
  await replaceWithRetry(temporary, path);
}

export async function replaceWithRetry(source, destination) {
  for (let attempt = 0; attempt < 6; attempt += 1) {
    try {
      await rename(source, destination);
      return;
    } catch (error) {
      if (attempt === 5 || !["EPERM", "EACCES"].includes(error.code)) throw error;
      await sleep(200 * (attempt + 1));
    }
  }
}

export function parseArgs(argv, definitions) {
  const values = { _: [] };
  for (const [name, definition] of Object.entries(definitions)) {
    if (Object.hasOwn(definition, "default")) values[name] = definition.default;
  }
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith("--")) {
      values._.push(token);
      continue;
    }
    const name = token.slice(2);
    const definition = definitions[name];
    if (!definition) throw new Error(`Unknown option: ${token}`);
    if (definition.type === "boolean") {
      values[name] = true;
      continue;
    }
    const raw = argv[++index];
    if (raw === undefined) throw new Error(`${token} requires a value`);
    values[name] = definition.type === "number" ? Number(raw) : raw;
    if (definition.type === "number" && !Number.isFinite(values[name])) throw new Error(`${token} requires a number`);
  }
  return values;
}

export function randomSuffix(length = 5) {
  const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
  return [...randomBytes(length)].map((value) => alphabet[value % alphabet.length]).join("");
}

export function sanitizeError(value) {
  return sanitizeStackTrace(value).slice(-4000);
}

export function sanitizeStackTrace(value) {
  return String(value)
    .replace(/mongodb(?:\+srv)?:\/\/[^\s"']+/gi, "[REDACTED_URI]")
    .replace(/("password"\s*:\s*")[^"]+/gi, "$1[REDACTED]");
}

export function runNode(script, args = [], options = {}) {
  return new Promise((resolvePromise, reject) => {
    const child = spawn(process.execPath, [script, ...args], {
      cwd: options.cwd, env: options.env ?? process.env,
      stdio: options.stdio ?? "inherit", windowsHide: true
    });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) resolvePromise();
      else reject(new Error(`${script} failed (${signal ?? `exit ${code}`})`));
    });
  });
}

export function errorName(error) {
  return error instanceof Error ? error.name : "Error";
}

export function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

export function isMain(importMetaUrl) {
  return resolve(process.argv[1] ?? "") === resolve(fileURLToPath(importMetaUrl));
}
