import { EmailClient } from "@azure/communication-email";

export const CONTROL_PLANE_FAILURE_RECIPIENT = "abramees@microsoft.com";

function requiredEnvironmentValue(env, name) {
  const value = String(env[name] ?? "").trim();
  if (!value) throw new Error(`${name} is required to send control-plane failure email`);
  return value;
}

export async function sendControlPlaneFailureEmail(
  failure,
  { env = process.env, createClient = (connectionString) => new EmailClient(connectionString) } = {}
) {
  const connectionString = requiredEnvironmentValue(
    env, "AZURE_COMMUNICATION_EMAIL_CONNECTION_STRING"
  );
  const senderAddress = requiredEnvironmentValue(
    env, "AZURE_COMMUNICATION_EMAIL_FROM_ADDRESS"
  );
  const controlPlaneOperationId =
    failure.controlPlaneOperationId ?? "not provided by the resource provider";
  const subject = `[Atlas resiliency] ${failure.operation} control-plane failure`;
  const content = [
    "MongoDB Atlas resiliency control-plane failure",
    "",
    `Run ID: ${failure.runId}`,
    `Operation: ${failure.operation}`,
    `Timestamp (UTC): ${failure.timestampUtc}`,
    `Error code: ${failure.errorCode}`,
    `Error message: ${failure.errorMessage}`,
    `Control-plane OperationId: ${controlPlaneOperationId}`,
    `Retry count: ${failure.retryCount}`,
    `Final outcome: ${failure.finalOutcome}`,
    "",
    "Stack trace:",
    failure.stackTrace
  ].join("\n");
  const client = createClient(connectionString);
  const poller = await client.beginSend({
    senderAddress,
    recipients: {
      to: [{ address: CONTROL_PLANE_FAILURE_RECIPIENT }]
    },
    content: { subject, plainText: content }
  });
  const result = await poller.pollUntilDone();
  if (result.status !== "Succeeded") {
    throw new Error(`Control-plane failure email completed with status ${result.status}`);
  }
  return result;
}
