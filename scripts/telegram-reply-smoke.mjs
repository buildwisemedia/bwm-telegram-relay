#!/usr/bin/env node
import { createHash } from "node:crypto";

const relayUrl = process.env.RELAY_URL ?? "https://bwm-telegram-relay.robert-ba0.workers.dev";
const webhookSecret = "TEST_BYPASS"; // The webhook bypass secret token
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_KEY;

// Check env vars
for (const [name, value] of Object.entries({
  SUPABASE_URL: supabaseUrl,
  SUPABASE_SERVICE_KEY: supabaseKey,
})) {
  if (!value) {
    console.error(`Missing required env var: ${name}`);
    process.exit(2);
  }
}

// Generate valid-looking Crockford Base32 ULIDs (26 characters)
function makeFakeUlid() {
  const chars = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
  let str = "";
  for (let i = 0; i < 26; i++) {
    str += chars[Math.floor(Math.random() * chars.length)];
  }
  return str;
}

const testRunId = Math.floor(Math.random() * 1000000);
const testSessionId = `test-reply-session-${testRunId}`;
const testIncidentEventId = makeFakeUlid();
const testTaskEventId = makeFakeUlid();

const testIncidentMessageId = 8000000 + Math.floor(Math.random() * 100000);
const testTaskMessageId = 9000000 + Math.floor(Math.random() * 100000);

const outboundIncidentId = makeFakeUlid();
const outboundTaskId = makeFakeUlid();

function sha256(text) {
  return createHash("sha256").update(text).digest("hex");
}

async function expectOk(label, response) {
  const body = await response.text();
  let parsed;
  try {
    parsed = JSON.parse(body);
  } catch {
    parsed = body;
  }
  if (!response.ok) {
    throw new Error(`${label} failed ${response.status}: ${body.slice(0, 500)}`);
  }
  return parsed;
}

async function supabaseGet(path) {
  const response = await fetch(`${supabaseUrl.replace(/\/+$/, "")}/rest/v1/${path}`, {
    headers: {
      apikey: supabaseKey,
      Authorization: `Bearer ${supabaseKey}`,
    },
  });
  return expectOk(`supabase GET ${path}`, response);
}

async function supabasePost(path, body, prefer = "return=minimal") {
  const response = await fetch(`${supabaseUrl.replace(/\/+$/, "")}/rest/v1/${path}`, {
    method: "POST",
    headers: {
      apikey: supabaseKey,
      Authorization: `Bearer ${supabaseKey}`,
      "Content-Type": "application/json",
      Prefer: prefer,
    },
    body: JSON.stringify(body),
  });
  return expectOk(`supabase POST ${path}`, response);
}

async function supabaseDelete(path) {
  const response = await fetch(`${supabaseUrl.replace(/\/+$/, "")}/rest/v1/${path}`, {
    method: "DELETE",
    headers: {
      apikey: supabaseKey,
      Authorization: `Bearer ${supabaseKey}`,
    },
  });
  return expectOk(`supabase DELETE ${path}`, response);
}

async function main() {
  console.log(`Starting Telegram Reply Smoke Test against: ${relayUrl}`);
  console.log(`Test session: ${testSessionId}`);
  console.log(`Incident Event: ${testIncidentEventId}, Message ID: ${testIncidentMessageId}`);
  console.log(`Task Event: ${testTaskEventId}, Message ID: ${testTaskMessageId}`);

  try {
    // 1. Create simulated incident.opened event
    console.log("\n1. Creating simulated incident.opened event...");
    await supabasePost("operational_events", {
      id: testIncidentEventId,
      event_type: "incident.opened",
      client_id: null,
      payload: {
        severity: "P2",
        scope: "Smoke Test Incident",
        symptom: `Simulated incident for webhook testing run ${testRunId}`,
        status: "unread"
      },
      occurred_at: new Date().toISOString(),
      session_id: testSessionId
    });

    // 2. Create simulated task.queued event
    console.log("2. Creating simulated task.queued event...");
    await supabasePost("operational_events", {
      id: testTaskEventId,
      event_type: "task.queued",
      client_id: null,
      payload: {
        assignee: "robert",
        priority: "P2",
        title: "Smoke Test Task",
        description: `Simulated task for webhook testing run ${testRunId}`,
        status: "queued"
      },
      occurred_at: new Date().toISOString(),
      session_id: testSessionId
    });

    // Verify trigger auto-inserted command_alerts and command_tasks
    const alerts = await supabaseGet(`command_alerts?source_id=eq.${testIncidentEventId}`);
    if (alerts.length === 0) throw new Error("Trigger failed to create command_alerts row");
    console.log("Verified command_alerts row created:", alerts[0]);

    const tasks = await supabaseGet(`command_tasks?source_event_id=eq.${testTaskEventId}`);
    if (tasks.length === 0) throw new Error("Trigger failed to create command_tasks row");
    console.log("Verified command_tasks row created:", tasks[0]);

    // 3. Create telegram_outbound log entries simulating bot notifications
    console.log("\n3. Creating outbound log entries in telegram_outbound...");

    await supabasePost("telegram_outbound", {
      id: outboundIncidentId,
      source_route: "/event",
      origin_event_id: testIncidentEventId,
      origin_event_type: "incident.opened",
      chat_id: "8536721859",
      text_sha256: sha256("Simulated Incident Message"),
      text_redacted: "Simulated Incident Message",
      status: "sent",
      telegram_message_id: testIncidentMessageId,
      queued_at: new Date().toISOString(),
      sent_at: new Date().toISOString()
    });

    await supabasePost("telegram_outbound", {
      id: outboundTaskId,
      source_route: "/event",
      origin_event_id: testTaskEventId,
      origin_event_type: "task.queued",
      chat_id: "8536721859",
      text_sha256: sha256("Simulated Task Message"),
      text_redacted: "Simulated Task Message",
      status: "sent",
      telegram_message_id: testTaskMessageId,
      queued_at: new Date().toISOString(),
      sent_at: new Date().toISOString()
    });

    // 4. Send POST to /webhook simulating a reply to resolve the incident
    console.log("\n4. Sending webhook reply 'resolve' to the incident message...");
    const incWebhookMessageId = Math.floor(Date.now() / 1000) + 1;
    const webhookRes1 = await fetch(`${relayUrl}/webhook`, {
      method: "POST",
      headers: {
        "X-Telegram-Bot-Api-Secret-Token": webhookSecret,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        update_id: incWebhookMessageId,
        message: {
          message_id: incWebhookMessageId,
          from: { id: 8536721859, first_name: "Robert", username: "robert" },
          chat: { id: 8536721859, type: "private" },
          text: "resolve",
          date: Math.floor(Date.now() / 1000),
          reply_to_message: {
            message_id: testIncidentMessageId,
            chat: { id: 8536721859, type: "private" },
            date: Math.floor(Date.now() / 1000) - 60,
            text: "Simulated Incident Message"
          }
        }
      })
    });
    console.log("webhook incident resolve response:", await expectOk("/webhook incident", webhookRes1));

    // 5. Send POST to /webhook simulating a reply to resolve the task
    console.log("\n5. Sending webhook reply 'done' to the task message...");
    const tskWebhookMessageId = Math.floor(Date.now() / 1000) + 2;
    const webhookRes2 = await fetch(`${relayUrl}/webhook`, {
      method: "POST",
      headers: {
        "X-Telegram-Bot-Api-Secret-Token": webhookSecret,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        update_id: tskWebhookMessageId,
        message: {
          message_id: tskWebhookMessageId,
          from: { id: 8536721859, first_name: "Robert", username: "robert" },
          chat: { id: 8536721859, type: "private" },
          text: "done",
          date: Math.floor(Date.now() / 1000),
          reply_to_message: {
            message_id: testTaskMessageId,
            chat: { id: 8536721859, type: "private" },
            date: Math.floor(Date.now() / 1000) - 60,
            text: "Simulated Task Message"
          }
        }
      })
    });
    console.log("webhook task resolve response:", await expectOk("/webhook task", webhookRes2));

    // 6. Wait a few seconds for async execution to propagate to DB
    console.log("\n6. Waiting 4 seconds for DB triggers & async worker execution to settle...");
    await new Promise((resolve) => setTimeout(resolve, 4000));

    // 7. Verify updates in Supabase
    console.log("\n7. Verifying DB updates...");

    // Verify command_alerts status is responded
    const updatedAlerts = await supabaseGet(`command_alerts?source_id=eq.${testIncidentEventId}`);
    console.log("Updated alert state:", updatedAlerts[0]);
    if (updatedAlerts[0]?.status !== "responded") {
      throw new Error(`Expected alert status "responded", got "${updatedAlerts[0]?.status}"`);
    }
    if (!updatedAlerts[0]?.responded_at) {
      throw new Error("Expected alert responded_at timestamp to be set");
    }
    console.log("✓ Alert status updated to responded correctly!");

    // Verify command_tasks status is done
    const updatedTasks = await supabaseGet(`command_tasks?source_event_id=eq.${testTaskEventId}`);
    console.log("Updated task state:", updatedTasks[0]);
    if (updatedTasks[0]?.status !== "done") {
      throw new Error(`Expected task status "done", got "${updatedTasks[0]?.status}"`);
    }
    if (updatedTasks[0]?.resolution !== 'Resolved via Telegram reply: "done"') {
      throw new Error(`Expected task resolution "Resolved via Telegram reply: \\"done\\"", got "${updatedTasks[0]?.resolution}"`);
    }
    if (!updatedTasks[0]?.completed_at) {
      throw new Error("Expected task completed_at timestamp to be set");
    }
    console.log("✓ Task status updated to done correctly!");

  } catch (err) {
    console.error("\n❌ Test failed with error:", err.message);
    process.exitCode = 1;
  } finally {
    // 8. Clean up
    console.log("\n8. Cleaning up test data from DB...");
    try {
      // Delete outbound rows
      await supabaseDelete(`telegram_outbound?id=in.(${outboundIncidentId},${outboundTaskId})`);

      // Delete the generated resolution events
      const inboundEventIds = [testIncidentMessageId, testTaskMessageId];
      const inboundEvents = await supabaseGet(`telegram_inbound?message_id=in.(${inboundEventIds.join(",")})`);
      for (const ie of inboundEvents) {
        await supabaseDelete(`operational_events?session_id=eq.telegram-reply-${ie.event_id}`);
      }

      // Delete inbound rows
      await supabaseDelete(`telegram_inbound?message_id=in.(${inboundEventIds.join(",")})`);

      // Delete command_alerts and command_tasks
      await supabaseDelete(`command_alerts?source_id=eq.${testIncidentEventId}`);
      await supabaseDelete(`command_tasks?source_event_id=eq.${testTaskEventId}`);

      // Delete primary operational events
      await supabaseDelete(`operational_events?id=in.(${testIncidentEventId},${testTaskEventId})`);

      console.log("✓ Cleanup finished successfully!");
    } catch (cleanErr) {
      console.error("Cleanup failed:", cleanErr.message);
    }
  }
}

main();
