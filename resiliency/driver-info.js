import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { version } = require("mongodb/package.json");

export function driverMetadata() {
  return {
    driver: "Node.js",
    driverVersion: `mongodb-${version}`
  };
}
