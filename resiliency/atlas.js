import { atomicJson, round, sanitizeError, sanitizeStackTrace, sleep, utcNow } from "./common.js";
import { STORAGE_SIZE_GB, validateComputeTransition, workloadThreads } from "./sku-config.js";
import { sendControlPlaneFailureEmail } from "./email-notifier.js";

export const ATLAS_AGENT = "MongoDB Atlas Resiliency Agent";
export const CAMPAIGN_REGIONS = Object.freeze(["eastus2", "westus2", "eastus", "northcentralus"]);
export const ATLAS_REGIONS = Object.freeze({
  uaecentral: "UAE_CENTRAL", uaenorth: "UAE_NORTH", eastasia: "ASIA_EAST",
  southeastasia: "ASIA_SOUTH_EAST", australiacentral: "AUSTRALIA_CENTRAL",
  australiacentral2: "AUSTRALIA_CENTRAL_2", australiaeast: "AUSTRALIA_EAST",
  australiasoutheast: "AUSTRALIA_SOUTH_EAST", brazilsouth: "BRAZIL_SOUTH",
  brazilsoutheast: "BRAZIL_SOUTHEAST", canadacentral: "CANADA_CENTRAL",
  canadaeast: "CANADA_EAST", germanywestcentral: "GERMANY_WEST_CENTRAL",
  northeurope: "EUROPE_NORTH", westeurope: "EUROPE_WEST", israelcentral: "ISRAEL_CENTRAL",
  centralindia: "INDIA_CENTRAL", southindia: "INDIA_SOUTH", westindia: "INDIA_WEST",
  japaneast: "JAPAN_EAST", japanwest: "JAPAN_WEST", koreacentral: "KOREA_CENTRAL",
  koreasouth: "KOREA_SOUTH", mexicocentral: "MEXICO_CENTRAL",
  newzealandnorth: "NEW_ZEALAND_NORTH", polandcentral: "POLAND_CENTRAL",
  swedencentral: "SWEDEN_CENTRAL", swedensouth: "SWEDEN_SOUTH",
  uksouth: "UK_SOUTH", ukwest: "UK_WEST", centralus: "US_CENTRAL",
  eastus: "US_EAST", eastus2: "US_EAST_2", northcentralus: "US_NORTH_CENTRAL",
  southcentralus: "US_SOUTH_CENTRAL", westus: "US_WEST", westus2: "US_WEST_2",
  westus3: "US_WEST_3", westcentralus: "US_WEST_CENTRAL"
});

export function atlasResourceIds(manifest) {
  const prefix = `/groups/${manifest.atlas.projectId}`;
  return [
    `${prefix}/clusters/${manifest.clusterName}`,
    `${prefix}/databaseUsers/admin/${manifest.atlas.username}`
  ];
}

export function requireAtlasProjectId(projectId) {
  if (!/^[a-f0-9]{24}$/i.test(projectId ?? "")) {
    throw new Error("ATLAS_PROJECT_ID must be a 24-character Atlas project ID");
  }
  return projectId;
}

export function validateAtlasManifest(manifest, { approved = true, env = process.env, cleanupOnly = false } = {}) {
  if (manifest.provider !== "atlas" || manifest.schemaVersion !== 3 ||
      !/^[A-Za-z0-9-]+$/.test(manifest.runId ?? "") ||
      !/^abinav-resiliency-agent-[a-z0-9]+-[a-z0-9]{5}$/.test(manifest.clusterName ?? "") ||
      !/^[a-f0-9]{24}$/i.test(manifest.atlas?.projectId ?? "") ||
      manifest.atlas.providerName !== "AZURE" ||
      manifest.atlas.regionName !== ATLAS_REGIONS[manifest.location] ||
      !manifest.atlas.regionName ||
      manifest.atlas.replicaSetNodeCount !== 3 ||
      manifest.atlas.username !== `resiliency-${manifest.runId}` ||
      manifest.apiVersion !== "2024-08-05") {
    throw new Error("Invalid Atlas project, region, topology or resource identity");
  }
  if (env.ATLAS_PROJECT_ID && env.ATLAS_PROJECT_ID !== manifest.atlas.projectId) {
    throw new Error("ATLAS_PROJECT_ID does not match the planned Atlas project");
  }
  if (manifest.approvals?.approvedAtlasProjectId !== manifest.atlas.projectId ||
      (approved && (!manifest.approvals.executionApproved || !manifest.approvals.cleanupPreapproved))) {
    throw new Error("Missing Atlas project or campaign approval");
  }
  const expected = atlasResourceIds(manifest);
  if (JSON.stringify(manifest.plannedResourceIds) !== JSON.stringify(expected) ||
      !Array.isArray(manifest.ownedResourceIds) ||
      manifest.ownedResourceIds.some((id) => !expected.includes(id))) {
    throw new Error("Atlas resources are outside the approved run");
  }
  const experiment = manifest.experiment;
  if (!cleanupOnly) {
    validateComputeTransition(experiment.initialComputeTier, experiment.targetComputeTier);
    if (!CAMPAIGN_REGIONS.includes(manifest.location)) {
      throw new Error(`Unapproved region for ${manifest.runId}: ${manifest.location}`);
    }
    if (experiment.storageSizeGb !== STORAGE_SIZE_GB) {
      throw new Error(`Unapproved storage size for ${manifest.runId}: expected ${STORAGE_SIZE_GB} GB`);
    }
  }
  if (experiment.highAvailability !== "AtlasReplicaSet" ||
      experiment.shardCount !== 1 || !["always-delete", "retain"].includes(experiment.cleanupPolicy) ||
      experiment.workloadThreads !== workloadThreads(experiment.initialComputeTier)) {
    throw new Error("Invalid Atlas experiment settings");
  }
  return manifest;
}

export class AtlasApiError extends Error {
  constructor(status, code, requestId = null) {
    // Response details can contain database credentials. Persist only the API error code.
    super(`Atlas API request failed: HTTP ${status} (${code})`);
    this.name = "AtlasApiError";
    this.status = status;
    this.code = code;
    this.controlPlaneOperationId = requestId;
  }
}

export class AtlasClient {
  constructor({ env = process.env, fetchImpl = fetch, wait = sleep, notify = sendControlPlaneFailureEmail } = {}) {
    this.env = env;
    this.clientId = env.ATLAS_CLIENT_ID;
    this.clientSecret = env.ATLAS_CLIENT_SECRET;
    if (!this.clientId || !this.clientSecret) {
      throw new Error("Set ATLAS_CLIENT_ID and ATLAS_CLIENT_SECRET in the process environment");
    }
    this.fetch = fetchImpl;
    this.wait = wait;
    this.notify = notify;
    this.token = null;
    this.tokenExpires = 0;
  }

  async accessToken() {
    if (this.token && Date.now() < this.tokenExpires) return this.token;
    const response = await this.fetch("https://cloud.mongodb.com/api/oauth/token", {
      method: "POST",
      headers: {
        Authorization: `Basic ${Buffer.from(`${this.clientId}:${this.clientSecret}`).toString("base64")}`,
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: "grant_type=client_credentials", signal: AbortSignal.timeout(30_000)
    });
    if (!response.ok) throw new AtlasApiError(response.status, "TokenRequestFailed");
    const value = await response.json();
    if (!value.access_token || !Number.isFinite(value.expires_in) || value.expires_in <= 60) {
      throw new Error("Atlas returned an invalid access token response");
    }
    this.token = value.access_token;
    this.tokenExpires = Date.now() + (value.expires_in - 60) * 1000;
    return this.token;
  }

  async request(method, path, { body, allowNotFound = false } = {}) {
    if (!/^\/groups\/[a-f0-9]{24}\//i.test(path) || path.includes("..") || path.includes("?")) {
      throw new Error("Invalid Atlas project API path");
    }
    const response = await this.fetch(`https://cloud.mongodb.com/api/atlas/v2${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${await this.accessToken()}`,
        Accept: path.includes("/databaseUsers")
          ? "application/vnd.atlas.2023-01-01+json" : "application/vnd.atlas.2024-08-05+json",
        "Content-Type": "application/json"
      },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(120_000)
    });
    if (allowNotFound && response.status === 404) return null;
    if (!response.ok) {
      const text = await response.text();
      let code = "RequestFailed";
      try {
        const value = JSON.parse(text);
        if (/^[A-Z0-9_]+$/.test(value.errorCode)) code = value.errorCode;
      } catch {
        code = "InvalidErrorResponse";
      }
      const id = response.headers.get("x-mongodb-request-id");
      throw new AtlasApiError(response.status, code, id);
    }
    const text = await response.text();
    return text ? JSON.parse(text) : {};
  }

  clusterBody(manifest, tier) {
    validateAtlasManifest(manifest, { env: this.env });
    if (![manifest.experiment.initialComputeTier, manifest.experiment.targetComputeTier].includes(tier)) {
      throw new Error(`Unapproved cluster tier: ${tier}`);
    }
    return {
      name: manifest.clusterName, clusterType: "REPLICASET",
      backupEnabled: false, pitEnabled: false,
      terminationProtectionEnabled: false,
      replicationSpecs: [{
        regionConfigs: [{
          providerName: "AZURE", regionName: manifest.atlas.regionName, priority: 7,
          electableSpecs: { instanceSize: tier, nodeCount: 3, diskSizeGB: manifest.experiment.storageSizeGb },
          autoScaling: { diskGB: { enabled: false }, compute: { enabled: false, scaleDownEnabled: false } }
        }]
      }],
      tags: [{ key: "purpose", value: "atlas-resiliency" }, { key: "runId", value: manifest.runId }]
    };
  }

  verifyCluster(resource, manifest) {
    if (resource.name !== manifest.clusterName || resource.groupId !== manifest.atlas.projectId ||
        !resource.tags?.some((tag) => tag.key === "runId" && tag.value === manifest.runId) ||
        !resource.tags?.some((tag) => tag.key === "purpose" && tag.value === "atlas-resiliency")) {
      throw new Error("Atlas cluster ownership verification failed");
    }
  }

  async scaleBody(manifest) {
    validateAtlasManifest(manifest, { env: this.env });
    const [path] = atlasResourceIds(manifest);
    const resource = await this.request("GET", path);
    this.verifyCluster(resource, manifest);
    if (resource.replicationSpecs?.length !== 1) throw new Error("Atlas scale requires exactly one replica set");
    const body = this.clusterBody(manifest, manifest.experiment.targetComputeTier);
    for (const key of ["id", "zoneId", "zoneName"]) {
      if (resource.replicationSpecs[0][key] !== undefined) {
        body.replicationSpecs[0][key] = resource.replicationSpecs[0][key];
      }
    }
    delete body.name;
    delete body.clusterType;
    return body;
  }

  async waitCluster(manifest, events, operation, tier, workload = null, stopped = () => false) {
    const [path] = atlasResourceIds(manifest);
    const deadline = performance.now() + 3_600_000;
    let previous;
    while (!stopped() && performance.now() < deadline) {
      if (workload && (workload.exitCode !== null || workload.signalCode !== null)) {
        throw new Error("Workload exited while the Atlas operation was active");
      }
      const resource = await this.request("GET", path);
      this.verifyCluster(resource, manifest);
      if (resource.stateName !== previous) {
        await events.write("state_transition", {
          operation, atlas_state: resource.stateName,
          provisioning_state: resource.stateName === "IDLE" ? "Succeeded" : "Updating",
          cluster_status: resource.stateName === "IDLE" ? "Ready" : resource.stateName
        });
        previous = resource.stateName;
      }
      if (resource.stateName === "IDLE") {
        const regions = resource.replicationSpecs?.flatMap((item) => item.regionConfigs ?? []) ?? [];
        if (resource.clusterType !== "REPLICASET" || regions.length !== 1 || regions[0].providerName !== "AZURE" ||
            regions[0].regionName !== manifest.atlas.regionName ||
            regions[0].electableSpecs?.instanceSize !== tier ||
            regions[0].electableSpecs?.nodeCount !== 3 ||
            regions[0].electableSpecs?.diskSizeGB !== manifest.experiment.storageSizeGb) {
          if (operation === "scale-up" && regions.length === 1 &&
              regions[0].electableSpecs?.instanceSize === manifest.experiment.initialComputeTier) {
            await this.wait(10_000);
            continue;
          }
          throw new Error("Atlas completed without the exact planned region, tier, storage and topology");
        }
        return resource;
      }
      if (["DELETING", "DELETED"].includes(resource.stateName)) {
        throw new Error(`Unexpected Atlas cluster state: ${resource.stateName}`);
      }
      await this.wait(10_000);
    }
    throw new Error(`${operation} did not complete within 3600 seconds or was interrupted`);
  }

  async provision(manifest, manifestPath, password, events, stopped) {
    validateAtlasManifest(manifest, { env: this.env });
    const [clusterPath, userPath] = atlasResourceIds(manifest);
    const cluster = await this.request("GET", clusterPath, { allowNotFound: true });
    const user = await this.request("GET", userPath, { allowNotFound: true });
    if ((cluster !== null && !manifest.ownedResourceIds.includes(clusterPath)) ||
        (user !== null && !manifest.ownedResourceIds.includes(userPath))) {
      throw new Error("Atlas run resource already exists; refusing to adopt it");
    }
    if (cluster !== null) this.verifyCluster(cluster, manifest);
    if (user !== null) this.verifyUser(user, manifest);
    // Persist creation intent first so an ambiguous POST response can still be cleaned up.
    manifest.ownedResourceIds = [clusterPath, userPath];
    manifest.status = "atlas-provision-submitted";
    await atomicJson(manifestPath, manifest);
    if (cluster === null) {
      await this.request("POST", `/groups/${manifest.atlas.projectId}/clusters`, {
        body: this.clusterBody(manifest, manifest.experiment.initialComputeTier)
      });
    }
    if (user === null) await this.request("POST", `/groups/${manifest.atlas.projectId}/databaseUsers`, {
      body: {
        databaseName: "admin", username: manifest.atlas.username, password,
        roles: [{ roleName: "readWrite", databaseName: "resiliencyTestDB" }],
        scopes: [{ name: manifest.clusterName, type: "CLUSTER" }]
      }
    });
    return this.waitCluster(
      manifest, events, "provision", manifest.experiment.initialComputeTier, null, stopped
    );
  }

  verifyUser(user, manifest) {
    if (user.username !== manifest.atlas.username ||
        user.databaseName !== "admin" || user.groupId !== manifest.atlas.projectId ||
        user.scopes?.length !== 1 || user.scopes[0].name !== manifest.clusterName ||
        user.scopes[0].type !== "CLUSTER") {
      throw new Error("Atlas database-user ownership verification failed");
    }
  }

  connectionString(resource, manifest, password) {
    let uri;
    try { uri = new URL(resource.connectionStrings?.standardSrv); }
    catch { throw new Error("Atlas did not return a valid SRV connection string"); }
    if (uri.protocol !== "mongodb+srv:" || !/^[a-z0-9.-]+$/i.test(uri.hostname) ||
        uri.username || uri.password || uri.port || !["", "/"].includes(uri.pathname) || uri.hash) {
      throw new Error("Atlas did not return a valid SRV connection string");
    }
    uri.username = encodeURIComponent(manifest.atlas.username);
    uri.password = encodeURIComponent(password);
    uri.pathname = "/";
    for (const [key, value] of [
      ["tls", "true"], ["authSource", "admin"], ["retryWrites", "false"], ["retryReads", "false"]
    ]) uri.searchParams.set(key, value);
    return uri.toString();
  }

  async cleanup(manifest, manifestPath, evidencePath) {
    validateAtlasManifest(manifest, { env: this.env, cleanupOnly: true });
    if (!manifest.ownedResourceIds.length) throw new Error("No Atlas resources are recorded as owned by this run");
    const [clusterPath, userPath] = atlasResourceIds(manifest);
    const submitted = utcNow();
    const started = performance.now();
    const evidence = {
      runId: manifest.runId, atlasProjectId: manifest.atlas.projectId,
      cleanupSubmittedUtc: submitted, cleanupCompletedUtc: null, cleanupTimeSeconds: null,
      verification: "Atlas cleanup pending", deletedOnlyRecordedOwnedResources: false
    };
    try {
      const failures = [];
      for (const path of [userPath, clusterPath]) {
        if (!manifest.ownedResourceIds.includes(path)) continue;
        try {
          const resource = await this.request("GET", path, { allowNotFound: true });
          if (resource !== null) {
            if (path === clusterPath) this.verifyCluster(resource, manifest);
            else this.verifyUser(resource, manifest);
            if (resource.stateName !== "DELETING") await this.request("DELETE", path);
          }
          const deadline = performance.now() + 3_600_000;
          while (await this.request("GET", path, { allowNotFound: true }) !== null) {
            if (performance.now() >= deadline) throw new Error("Atlas resource deletion was not verified within 3600 seconds");
            await this.wait(10_000);
          }
        } catch (error) {
          failures.push(error);
        }
      }
      if (failures.length) {
        const error = new AggregateError(failures, failures.map((failure) => failure.message).join("; "));
        error.code = failures[0].code ?? failures[0].name;
        error.controlPlaneOperationId = failures[0].controlPlaneOperationId ?? null;
        throw error;
      }
      Object.assign(evidence, {
        cleanupCompletedUtc: utcNow(), cleanupTimeSeconds: round((performance.now() - started) / 1000),
        verification: "Every recorded Atlas cluster and database user returned 404",
        deletedOnlyRecordedOwnedResources: true
      });
      await atomicJson(evidencePath, evidence);
      manifest.cleanup = evidence;
      await atomicJson(manifestPath, manifest);
      return evidence;
    } catch (error) {
      evidence.cleanupTimeSeconds = round((performance.now() - started) / 1000);
      evidence.failure = {
        operation: "cleanup", timestampUtc: utcNow(), errorCode: String(error.code ?? error.name),
        errorMessage: sanitizeError(error.message), controlPlaneOperationId: error.controlPlaneOperationId ?? null,
        retryCount: 0, finalOutcome: "failed", stackTrace: sanitizeStackTrace(error.stack)
      };
      await atomicJson(evidencePath, evidence);
      try {
        await this.notify({ ...evidence.failure, runId: manifest.runId, provider: "atlas" });
      } catch (notificationError) {
        evidence.notificationFailure = { errorCategory: notificationError.name };
        await atomicJson(evidencePath, evidence);
        console.error(`Atlas cleanup failure email could not be sent (${notificationError.name})`);
      }
      throw error;
    }
  }
}
