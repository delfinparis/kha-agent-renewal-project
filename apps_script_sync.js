// ============================================
// MONDAY.COM TERMINATED AGENT SYNC
// Add this below the existing Cold Email Drip Sender code
// Removes terminated agents from the email list daily
// ============================================

// --- MONDAY.COM CONFIG (KHA) ---
const MONDAY_BOARD_ID = 359616654;
const MONDAY_TERMINATED_GROUP_ID = "new_group32247";
const MONDAY_ENTITY = "KHA";

// --- MONDAY.COM API ---

function mondayQuery(query, variables) {
  const token = PropertiesService.getScriptProperties().getProperty("MONDAY_API_TOKEN");
  if (!token) throw new Error("Set MONDAY_API_TOKEN in Script Properties (Project Settings > Script Properties)");

  const resp = UrlFetchApp.fetch("https://api.monday.com/v2", {
    method: "post",
    contentType: "application/json",
    headers: { Authorization: token, "API-Version": "2024-10" },
    payload: JSON.stringify({ query: query, variables: variables || {} }),
    muteHttpExceptions: true,
  });

  const data = JSON.parse(resp.getContentText());
  if (data.errors) throw new Error("Monday.com API error: " + JSON.stringify(data.errors));
  return data.data;
}

function getTerminatedAgents() {
  const agents = [];
  let cursor = null;
  let isFirstPage = true;

  while (true) {
    let data;

    if (isFirstPage) {
      data = mondayQuery(
        `query ($boardId: [ID!]!) {
          boards(ids: $boardId) {
            items_page(limit: 500) {
              cursor
              items { name, group { id }, column_values { title text } }
            }
          }
        }`,
        { boardId: [String(MONDAY_BOARD_ID)] }
      );
      const page = data.boards[0].items_page;
      cursor = page.cursor;
      page.items.forEach(item => {
        if (item.group.id === MONDAY_TERMINATED_GROUP_ID) agents.push(parseMonday(item));
      });
      isFirstPage = false;
    } else {
      data = mondayQuery(
        `query ($cursor: String!) {
          next_items_page(limit: 500, cursor: $cursor) {
            cursor
            items { name, group { id }, column_values { title text } }
          }
        }`,
        { cursor: cursor }
      );
      const page = data.next_items_page;
      cursor = page.cursor;
      page.items.forEach(item => {
        if (item.group.id === MONDAY_TERMINATED_GROUP_ID) agents.push(parseMonday(item));
      });
    }

    if (!cursor) break;
  }

  return agents;
}

function parseMonday(item) {
  const cols = {};
  item.column_values.forEach(c => { cols[c.title.toLowerCase()] = c.text; });

  let first = cols["first name"] || cols["firstname"] || cols["first"] || "";
  let last = cols["last name"] || cols["lastname"] || cols["last"] || "";

  if (!first && !last) {
    const parts = item.name.trim().split(/\s+/);
    first = parts[0] || "";
    last = parts.slice(1).join(" ") || "";
  }

  return { first: first.trim().toLowerCase(), last: last.trim().toLowerCase(), raw: item.name };
}

// --- SYNC: REMOVE TERMINATED AGENTS FROM SHEET ---

function syncTerminatedAgents() {
  Logger.log("[" + MONDAY_ENTITY + "] Terminated agent sync starting...");

  const terminated = getTerminatedAgents();
  Logger.log("Terminated agents in Monday.com: " + terminated.length);

  if (terminated.length === 0) {
    Logger.log("No terminated agents found. Sheet unchanged.");
    return;
  }

  // Build lookup
  const terminatedSet = {};
  terminated.forEach(agent => {
    terminatedSet[agent.first + "|" + agent.last] = true;
    Logger.log("  " + agent.raw);
  });

  // Use existing getSpreadsheet() and getColumnMap() from drip sender
  const ss = getSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.SHEET_NAME);
  const colMap = getColumnMap(sheet);

  const firstIdx = colMap["first name"];
  const lastIdx = colMap["last name"];
  const emailIdx = colMap["email"];

  if (firstIdx === undefined || lastIdx === undefined) {
    Logger.log("ERROR: Could not find 'First Name' or 'Last Name' columns");
    return;
  }

  const data = sheet.getDataRange().getValues();
  const rowsToRemove = [];

  // Scan bottom-up so row indices stay valid during deletion
  for (let i = data.length - 1; i >= 1; i--) {
    const first = data[i][firstIdx].toString().trim().toLowerCase();
    const last = data[i][lastIdx].toString().trim().toLowerCase();

    if (terminatedSet[first + "|" + last]) {
      const email = emailIdx !== undefined ? data[i][emailIdx] : "";
      rowsToRemove.push({ row: i + 1, first: data[i][firstIdx], last: data[i][lastIdx], email: email });
    }
  }

  Logger.log("Rows to remove from sheet: " + rowsToRemove.length);

  if (rowsToRemove.length === 0) {
    Logger.log("No matching terminated agents found in sheet.");
    return;
  }

  // Delete rows (already in reverse order)
  rowsToRemove.forEach(match => {
    Logger.log("  Removing row " + match.row + ": " + match.first + " " + match.last + " (" + match.email + ")");
    sheet.deleteRow(match.row);
  });

  Logger.log("Done. Removed " + rowsToRemove.length + " terminated agent(s) from email list.");
}

// --- UPDATE THE EXISTING MENU ---
// Replace the existing onOpen() function with this one,
// or just add the sync items to your existing menu:

/*
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("Email Campaign")
    .addItem("Start Campaign", "startCampaign")
    .addItem("Pause Campaign", "pauseCampaign")
    .addItem("Check Status", "checkStatus")
    .addItem("Full Reset", "fullReset")
    .addSeparator()
    .addItem("Sync Terminated Agents (Monday.com)", "syncTerminatedAgents")
    .addToUi();
}
*/

// --- SETUP DAILY TRIGGER ---
// Run this ONCE to create a daily trigger for the sync:

function setupSyncTrigger() {
  // Remove any existing sync triggers
  ScriptApp.getProjectTriggers().forEach(trigger => {
    if (trigger.getHandlerFunction() === "syncTerminatedAgents") {
      ScriptApp.deleteTrigger(trigger);
    }
  });

  // Create daily trigger at 5am-6am (runs before the email campaign)
  ScriptApp.newTrigger("syncTerminatedAgents")
    .timeBased()
    .everyDays(1)
    .atHour(5)
    .create();

  Logger.log("Daily sync trigger created (5am-6am).");
}
