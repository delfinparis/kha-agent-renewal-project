/**
 * KHA — Daily Email List Sync with Monday.com
 *
 * Checks the Monday.com board for agents in the "terminated" group
 * and removes matching rows from this Google Sheet.
 *
 * Setup:
 * 1. Open this Google Sheet
 * 2. Extensions > Apps Script
 * 3. Paste this entire script, replacing any existing code
 * 4. Click the gear icon (Project Settings) > Script Properties
 *    Add: MONDAY_API_TOKEN = your Monday.com API token
 * 5. Run syncEmailList() once manually to authorize
 * 6. Triggers > Add Trigger:
 *    - Function: syncEmailList
 *    - Event source: Time-driven
 *    - Type: Day timer
 *    - Time: 6am to 7am (or your preferred time)
 */

// === CONFIGURATION (KHA) ===
const BOARD_ID = 359616654;
const TERMINATED_GROUP_ID = "new_group32247";
const ENTITY_NAME = "KHA";

// === MONDAY.COM API ===

function mondayQuery(query, variables) {
  const token = PropertiesService.getScriptProperties().getProperty("MONDAY_API_TOKEN");
  if (!token) throw new Error("Set MONDAY_API_TOKEN in Script Properties");

  const options = {
    method: "post",
    contentType: "application/json",
    headers: { Authorization: token, "API-Version": "2024-10" },
    payload: JSON.stringify({ query: query, variables: variables || {} }),
    muteHttpExceptions: true,
  };

  const resp = UrlFetchApp.fetch("https://api.monday.com/v2", options);
  const data = JSON.parse(resp.getContentText());

  if (data.errors) {
    throw new Error("Monday.com API error: " + JSON.stringify(data.errors));
  }
  return data.data;
}

function getTerminatedAgents() {
  const agents = [];
  let cursor = null;
  let isFirstPage = true;

  while (true) {
    let data;

    if (isFirstPage) {
      const query = `query ($boardId: [ID!]!) {
        boards(ids: $boardId) {
          items_page(limit: 500) {
            cursor
            items {
              name
              group { id }
              column_values { title text }
            }
          }
        }
      }`;
      data = mondayQuery(query, { boardId: [String(BOARD_ID)] });
      const page = data.boards[0].items_page;
      cursor = page.cursor;

      page.items.forEach(function(item) {
        if (item.group.id === TERMINATED_GROUP_ID) {
          agents.push(parseAgentName(item));
        }
      });
      isFirstPage = false;
    } else {
      const query = `query ($cursor: String!) {
        next_items_page(limit: 500, cursor: $cursor) {
          cursor
          items {
            name
            group { id }
            column_values { title text }
          }
        }
      }`;
      data = mondayQuery(query, { cursor: cursor });
      const page = data.next_items_page;
      cursor = page.cursor;

      page.items.forEach(function(item) {
        if (item.group.id === TERMINATED_GROUP_ID) {
          agents.push(parseAgentName(item));
        }
      });
    }

    if (!cursor) break;
  }

  return agents;
}

function parseAgentName(item) {
  const cols = {};
  item.column_values.forEach(function(c) {
    cols[c.title.toLowerCase()] = c.text;
  });

  let first = cols["first name"] || cols["firstname"] || cols["first"] || "";
  let last = cols["last name"] || cols["lastname"] || cols["last"] || "";

  // Fallback: parse item name
  if (!first && !last) {
    const parts = item.name.trim().split(/\s+/);
    first = parts[0] || "";
    last = parts.slice(1).join(" ") || "";
  }

  return { first: first.trim().toLowerCase(), last: last.trim().toLowerCase() };
}

// === SYNC LOGIC ===

function syncEmailList() {
  Logger.log("[" + ENTITY_NAME + "] Email list sync starting...");

  // Step 1: Get terminated agents from Monday.com
  const terminated = getTerminatedAgents();
  Logger.log("Terminated agents found: " + terminated.length);

  if (terminated.length === 0) {
    Logger.log("No terminated agents. Sheet unchanged.");
    return;
  }

  // Build lookup set
  const terminatedSet = {};
  terminated.forEach(function(agent) {
    const key = agent.first + "|" + agent.last;
    terminatedSet[key] = true;
    Logger.log("  Terminated: " + agent.first + " " + agent.last);
  });

  // Step 2: Read sheet data
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const data = sheet.getDataRange().getValues();
  const headers = data[0].map(function(h) { return h.toString().toLowerCase().trim(); });

  const firstIdx = headers.indexOf("first name");
  const lastIdx = headers.indexOf("last name");

  if (firstIdx === -1 || lastIdx === -1) {
    Logger.log("ERROR: Could not find 'First Name' or 'Last Name' columns");
    return;
  }

  // Step 3: Find rows to remove (bottom-up)
  const rowsToRemove = [];
  for (var i = data.length - 1; i >= 1; i--) {
    const first = data[i][firstIdx].toString().trim().toLowerCase();
    const last = data[i][lastIdx].toString().trim().toLowerCase();
    const key = first + "|" + last;

    if (terminatedSet[key]) {
      rowsToRemove.push({ row: i + 1, first: data[i][firstIdx], last: data[i][lastIdx] });
    }
  }

  Logger.log("Rows to remove: " + rowsToRemove.length);

  // Step 4: Delete rows (already in reverse order)
  rowsToRemove.forEach(function(match) {
    Logger.log("  Removing row " + match.row + ": " + match.first + " " + match.last);
    sheet.deleteRow(match.row);
  });

  Logger.log("Done. Removed " + rowsToRemove.length + " agent(s).");
}
