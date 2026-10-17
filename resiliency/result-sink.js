import { readFile } from "node:fs/promises";
import { MongoClient, MongoServerError } from "mongodb";
import { readSecret, sleep } from "./common.js";

export const DATABASE = "resiliencyResultsDB";
export const COLLECTION = "atlasRuns";
export const IDENTITY_FIELDS = [
  "_id", "clusterName", "region",
  "initialComputeTier", "targetComputeTier", "storageSizeGb", "provider", "atlasProjectId"
];

function clone(value) {
  return structuredClone(value);
}

function equal(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function isDuplicateKey(error) {
  return error instanceof MongoServerError && error.code === 11000;
}

export class ResultSink {
  constructor(collection, client = null) {
    this.collection = collection;
    this.client = client;
  }

  static async connect({ uriEnv = "ATLAS_RESULTS_URI", promptIfMissing = false } = {}) {
    let uri = (process.env[uriEnv] ?? "").trim();
    if (!uri && promptIfMissing) {
      uri = (await readSecret("Long-lived MongoDB-compatible result connection string: ")).trim();
    }
    if (!uri) {
      throw new Error(`${uriEnv} is not set; set it in the process environment or use --prompt-results-uri`);
    }
    delete process.env[uriEnv];
    const client = new MongoClient(uri, {
      connectTimeoutMS: 10_000,
      serverSelectionTimeoutMS: 10_000,
      socketTimeoutMS: 30_000,
      retryWrites: false
    });
    await client.db("admin").command({ ping: 1 });
    return new ResultSink(client.db(DATABASE).collection(COLLECTION), client);
  }

  async close() {
    await this.client?.close();
  }

  async ensureInitial(document) {
    const runId = ResultSink.runId(document);
    try {
      await this.collection.insertOne(clone(document));
    } catch (error) {
      if (!isDuplicateKey(error)) throw error;
      const existing = await this.collection.findOne({ _id: runId });
      if (!equal(existing, document)) {
        throw new Error(`Result sink already contains a different document for ${runId}`);
      }
    }
    await this.verify(document);
  }

  async ensureResumeBase(initial) {
    const runId = ResultSink.runId(initial);
    try {
      await this.collection.insertOne(clone(initial));
      return clone(initial);
    } catch (error) {
      if (!isDuplicateKey(error)) throw error;
    }
    const existing = await this.collection.findOne({ _id: runId });
    if (!existing || typeof existing !== "object") {
      throw new Error(`Result sink could not read the existing document for ${runId}`);
    }
    if (IDENTITY_FIELDS.some((field) => existing[field] !== initial[field])) {
      throw new Error(`Result sink contains conflicting run identity for ${runId}`);
    }
    return existing;
  }

  async replaceAndVerify(document) {
    const runId = ResultSink.runId(document);
    const result = await this.collection.replaceOne({ _id: runId }, clone(document), { upsert: false });
    if (result.matchedCount !== 1) {
      throw new Error(`Result sink initial document is missing for ${runId}`);
    }
    await this.verify(document);
  }

  async replaceFinalAndVerify(
    document,
    { retrySeconds = 120, now = () => performance.now() / 1000, wait = sleep } = {}
  ) {
    if (retrySeconds < 0) throw new Error("retrySeconds must be non-negative");
    const deadline = now() + retrySeconds;
    let delay = 1;
    while (true) {
      try {
        await this.replaceAndVerify(document);
        return;
      } catch (error) {
        const retryable = error?.name?.startsWith("Mongo") ||
          (error instanceof Error && error.message.includes("readback differs"));
        if (!retryable) throw error;
      }
      const remaining = deadline - now();
      if (remaining <= 0) {
        throw new Error(`Final result persistence did not succeed within ${retrySeconds} seconds`);
      }
      await wait(Math.min(delay, remaining) * 1000);
      delay = Math.min(delay * 2, 10);
    }
  }

  async verify(expected) {
    const runId = ResultSink.runId(expected);
    const actual = await this.collection.findOne({ _id: runId });
    if (!equal(actual, expected)) {
      throw new Error(`Result-sink readback differs from the candidate for ${runId}`);
    }
  }

  static async load(path) {
    const value = JSON.parse(await readFile(path, "utf8"));
    if (!value || Array.isArray(value) || typeof value !== "object") {
      throw new Error(`Result document at ${path} must be an object`);
    }
    return value;
  }

  static runId(document) {
    if (typeof document._id !== "string" || !document._id) {
      throw new Error("Result document has no valid _id");
    }
    return document._id;
  }
}
