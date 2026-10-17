# Existing Tech Stack

- Language/runtime: JavaScript ES modules on Node.js 20.19 or newer.
- Package manager: npm with `package-lock.json`.
- MongoDB access: `mongodb` 7.6.0.
- BSON encoding: `BSON` exported by `mongodb` 7.6.0.
- JSON Schema: `ajv` 8.20.0 and `ajv-formats` 3.0.1.
- Tests: built-in `node:test`.
- Azure control plane: Azure CLI `az rest` against ARM.
- Infrastructure: subscription-scope ARM JSON template.
