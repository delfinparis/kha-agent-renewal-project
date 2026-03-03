// ============================================
// MONDAY.COM ACTIVE ROSTER SYNC
// Add this below the existing Cold Email Drip Sender code
// Compares Monday.com active agents against Google Sheets —
// anyone NOT on the active roster gets removed from the email list
// ============================================

// --- MONDAY.COM CONFIG (KHA) ---
const MONDAY_BOARD_ID = 359616654;
const MONDAY_ACTIVE_GROUP_ID = "group_title";
const MONDAY_ENTITY = "KHA";

// --- GITHUB CONFIG (for renewal report email) ---
const GITHUB_REPO_OWNER = "delfinparis";
const GITHUB_REPO_NAME = "kha-agent-renewal-project";
const REPORT_RECIPIENTS = ["dj@kalerealty.com", "rea@kalerealty.com"];

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

function getActiveAgents() {
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
        if (item.group.id === MONDAY_ACTIVE_GROUP_ID) agents.push(parseMonday(item));
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
        if (item.group.id === MONDAY_ACTIVE_GROUP_ID) agents.push(parseMonday(item));
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

// --- SYNC: REMOVE AGENTS NOT ON ACTIVE ROSTER ---

function syncActiveRoster() {
  Logger.log("[" + MONDAY_ENTITY + "] Active roster sync starting...");

  const activeAgents = getActiveAgents();
  Logger.log("Active agents on Monday.com: " + activeAgents.length);

  if (activeAgents.length === 0) {
    Logger.log("WARNING: No active agents found on Monday.com. Skipping sync to prevent accidental wipe.");
    return;
  }

  // Build lookup of active agents
  const activeSet = {};
  activeAgents.forEach(agent => {
    activeSet[agent.first + "|" + agent.last] = true;
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

    // If this person is NOT in the active roster, remove them
    if (!activeSet[first + "|" + last]) {
      const email = emailIdx !== undefined ? data[i][emailIdx] : "";
      rowsToRemove.push({ row: i + 1, first: data[i][firstIdx], last: data[i][lastIdx], email: email });
    }
  }

  Logger.log("Agents NOT on active roster (to remove): " + rowsToRemove.length);

  if (rowsToRemove.length === 0) {
    Logger.log("All sheet agents are on the active roster. No changes needed.");
    return;
  }

  // Delete rows (already in reverse order)
  rowsToRemove.forEach(match => {
    Logger.log("  Removing row " + match.row + ": " + match.first + " " + match.last + " (" + match.email + ")");
    sheet.deleteRow(match.row);
  });

  Logger.log("Done. Removed " + rowsToRemove.length + " agent(s) no longer on the active roster.");
}

// --- EMAIL RENEWAL REPORT ---

function emailRenewalReport() {
  Logger.log("[" + MONDAY_ENTITY + "] Fetching renewal report from GitHub...");

  var url = "https://raw.githubusercontent.com/" + GITHUB_REPO_OWNER + "/" + GITHUB_REPO_NAME + "/main/latest_report.txt";

  try {
    var resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true });

    if (resp.getResponseCode() !== 200) {
      Logger.log("No report found at " + url + " (HTTP " + resp.getResponseCode() + "). Skipping email.");
      return;
    }

    var reportText = resp.getContentText();
    if (!reportText || reportText.trim().length === 0) {
      Logger.log("Report is empty. Skipping email.");
      return;
    }

    var today = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");
    var subject = "[" + MONDAY_ENTITY + "] License Renewal Report — " + today;

    var htmlBody = "<h2>" + subject + "</h2>"
      + "<pre style='font-family: Consolas, monospace; font-size: 13px; line-height: 1.4;'>"
      + reportText.replace(/</g, "&lt;").replace(/>/g, "&gt;")
      + "</pre>"
      + "<hr><p style='color: #888; font-size: 11px;'>Auto-generated by GitHub Actions + Apps Script</p>";

    REPORT_RECIPIENTS.forEach(function(email) {
      GmailApp.sendEmail(email, subject, reportText, { htmlBody: htmlBody });
      Logger.log("Report emailed to: " + email);
    });

    Logger.log("Done. Report sent to " + REPORT_RECIPIENTS.length + " recipient(s).");
  } catch (e) {
    Logger.log("ERROR fetching/sending report: " + e.message);
  }
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
    .addItem("Sync Active Roster (Monday.com)", "syncActiveRoster")
    .addItem("Email Renewal Report", "emailRenewalReport")
    .addToUi();
}
*/

// --- SETUP DAILY TRIGGERS ---
// Run this ONCE to create daily triggers:

function setupSyncTrigger() {
  // Remove any existing sync triggers
  ScriptApp.getProjectTriggers().forEach(trigger => {
    var fn = trigger.getHandlerFunction();
    if (fn === "syncActiveRoster" || fn === "syncTerminatedAgents" || fn === "emailRenewalReport") {
      ScriptApp.deleteTrigger(trigger);
    }
  });

  // Daily roster sync at 5am (runs before the email campaign)
  ScriptApp.newTrigger("syncActiveRoster")
    .timeBased()
    .everyDays(1)
    .atHour(5)
    .create();

  // Daily report email at 10am (after GitHub Actions runs at ~2am CT)
  ScriptApp.newTrigger("emailRenewalReport")
    .timeBased()
    .everyDays(1)
    .atHour(10)
    .create();

  Logger.log("Daily triggers created: syncActiveRoster (5am), emailRenewalReport (10am).");
}
