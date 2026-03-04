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
              items { name, group { id }, column_values { id text } }
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
            items { name, group { id }, column_values { id text } }
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
  item.column_values.forEach(c => { cols[c.id.toLowerCase()] = c.text; });

  let first = cols["text95"] || "";
  let last = cols["text_19"] || "";

  if (!first && !last) {
    const parts = item.name.trim().split(/\s+/);
    first = parts[0] || "";
    last = parts.slice(1).join(" ") || "";
  }

  // License number: "license_number" for KHA, "license_number3" for HC
  var licenseNum = cols["license_number"] || cols["license_number3"] || "";

  // Email: work_email, fall back to home_email
  var email = cols["work_email"] || cols["home_email"] || "";

  // Phone
  var phone = cols["phone_number8"] || "";

  return {
    first: first.trim().toLowerCase(),
    last: last.trim().toLowerCase(),
    raw: item.name,
    license: licenseNum.trim(),
    email: email.trim(),
    phone: phone.trim()
  };
}

// --- SPREADSHEET CONFIG ---
// Uses CONFIG.SPREADSHEET_ID and CONFIG.SHEET_NAME from the drip sender

// --- DFPR LICENSE LOOKUP ---

function dfprLookup(licenseNumbers) {
  // Query DFPR by license numbers in batches of 100
  var results = {};
  var BATCH = 100;

  for (var i = 0; i < licenseNumbers.length; i += BATCH) {
    var batch = licenseNumbers.slice(i, i + BATCH);
    var inList = batch.map(function(n) { return "'" + n + "'"; }).join(", ");
    var where = "license_number IN (" + inList + ")";
    var url = "https://data.illinois.gov/resource/pzzh-kp68.json"
      + "?$where=" + encodeURIComponent(where)
      + "&$select=" + encodeURIComponent("license_number,expiration_date,license_status")
      + "&$limit=50000";

    var resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    if (resp.getResponseCode() === 200) {
      var records = JSON.parse(resp.getContentText());
      records.forEach(function(r) {
        // Keep the latest expiration per license number
        var num = r.license_number;
        var exp = r.expiration_date || "";
        if (!results[num] || exp > (results[num].expiration_date || "")) {
          results[num] = r;
        }
      });
    }
  }

  return results;
}

// --- SYNC: FULL ROSTER SYNC ---

function syncActiveRoster() {
  Logger.log("[" + MONDAY_ENTITY + "] Active roster sync starting...");

  var activeAgents = getActiveAgents();
  Logger.log("Active agents on Monday.com: " + activeAgents.length);

  if (activeAgents.length === 0) {
    Logger.log("WARNING: No active agents found. Skipping sync to prevent accidental wipe.");
    return;
  }

  // Build lookup of active agents by email
  var activeByEmail = {};
  activeAgents.forEach(function(agent) {
    if (agent.email) {
      activeByEmail[agent.email.toLowerCase()] = agent;
    }
  });
  Logger.log("Active agents with email: " + Object.keys(activeByEmail).length);

  // Open spreadsheet
  var ss = CONFIG.SPREADSHEET_ID
    ? SpreadsheetApp.openById(CONFIG.SPREADSHEET_ID)
    : SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(CONFIG.SHEET_NAME);
  var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  var colMap = {};
  headers.forEach(function(h, i) { colMap[h.toString().toLowerCase().trim()] = i; });

  var firstIdx = colMap["first name"];
  var emailIdx = colMap["email"];

  if (emailIdx === undefined) {
    Logger.log("ERROR: Could not find 'Email' column");
    return;
  }

  // --- PHASE 1: REMOVE rows ---
  var data = sheet.getDataRange().getValues();
  var rowsToRemove = [];
  var sheetEmails = {}; // track who's already on the sheet

  for (var i = data.length - 1; i >= 1; i--) {
    var email = data[i][emailIdx].toString().trim().toLowerCase();
    if (email) sheetEmails[email] = true;

    var reason = "";
    var agent = activeByEmail[email];

    // Remove if email not found on active roster
    if (!email || !agent) {
      reason = "not on active roster";
    }
    // Remove if license starts with 471 or 473 (no CE renewal needed)
    else if (agent.license && (agent.license.substring(0, 3) === "471" || agent.license.substring(0, 3) === "473")) {
      reason = "471/473 license (no CE renewal)";
    }

    if (reason) {
      var firstName = firstIdx !== undefined ? data[i][firstIdx] : "";
      rowsToRemove.push({ row: i + 1, first: firstName, email: email, reason: reason });
    }
  }

  Logger.log("Rows to remove: " + rowsToRemove.length);
  rowsToRemove.forEach(function(match) {
    Logger.log("  Remove row " + match.row + ": " + match.first + " (" + match.email + ") — " + match.reason);
    sheet.deleteRow(match.row);
  });

  // --- PHASE 1.5: REMOVE agents who have already renewed ---
  // Re-read sheet after Phase 1 deletions
  data = sheet.getDataRange().getValues();
  var renewalCheck = [];

  for (var j = 1; j < data.length; j++) {
    var checkEmail = data[j][emailIdx].toString().trim().toLowerCase();
    var checkAgent = activeByEmail[checkEmail];
    if (checkAgent && checkAgent.license && checkAgent.license.substring(0, 3) === "475") {
      renewalCheck.push({ row: j + 1, email: checkEmail, first: firstIdx !== undefined ? data[j][firstIdx] : "", license: checkAgent.license });
    }
  }

  if (renewalCheck.length > 0) {
    var checkLicNums = renewalCheck.map(function(r) { return r.license; });
    Logger.log("Checking " + checkLicNums.length + " existing agents against DFPR for renewal status...");
    var dfprRenewalData = dfprLookup(checkLicNums);

    var renewedRows = [];
    renewalCheck.forEach(function(check) {
      var dfpr = dfprRenewalData[check.license];
      if (dfpr && dfpr.expiration_date && dfpr.expiration_date.indexOf("04/30/2026") !== 0) {
        check.exp = dfpr.expiration_date;
        renewedRows.push(check);
      }
    });

    Logger.log("Agents already renewed (to remove): " + renewedRows.length);
    // Sort by row descending for safe deletion
    renewedRows.sort(function(a, b) { return b.row - a.row; });
    renewedRows.forEach(function(match) {
      Logger.log("  Remove row " + match.row + ": " + match.first + " (" + match.email + ") — already renewed (exp " + match.exp + ")");
      sheet.deleteRow(match.row);
    });
  }

  // Rebuild sheetEmails after all removals
  data = sheet.getDataRange().getValues();
  sheetEmails = {};
  for (var k = 1; k < data.length; k++) {
    var rebuildEmail = data[k][emailIdx].toString().trim().toLowerCase();
    if (rebuildEmail) sheetEmails[rebuildEmail] = true;
  }

  // --- PHASE 2: ADD new 475 agents who need renewal ---
  // Find active agents with 475 licenses NOT already on sheet
  var newAgents475 = [];
  activeAgents.forEach(function(agent) {
    if (agent.email && !sheetEmails[agent.email.toLowerCase()] && agent.license && agent.license.substring(0, 3) === "475") {
      newAgents475.push(agent);
    }
  });

  Logger.log("New agents with 475 license not on sheet: " + newAgents475.length);

  if (newAgents475.length > 0) {
    // Check DFPR for these license numbers
    var licNums = newAgents475.map(function(a) { return a.license; });
    Logger.log("Checking " + licNums.length + " license numbers against DFPR...");
    var dfprData = dfprLookup(licNums);

    var addedCount = 0;
    // Sheet columns: First Name, Email, Status, Timestamp, Phone, Last Name, Last Name 2, Last Name 3
    newAgents475.forEach(function(agent) {
      var dfpr = dfprData[agent.license];
      if (!dfpr) {
        Logger.log("  " + agent.raw + " (lic " + agent.license + "): not found in DFPR, skipping");
        return;
      }

      var exp = dfpr.expiration_date || "";
      // Check if expiration is 4/30/2026 (needs renewal)
      if (exp.indexOf("04/30/2026") === 0) {
        // Capitalize name for the sheet
        var firstName = agent.first.charAt(0).toUpperCase() + agent.first.slice(1);

        // Append row: First Name, Email, Status, Timestamp, Phone
        sheet.appendRow([firstName, agent.email, "", "", agent.phone]);
        Logger.log("  ADDED: " + firstName + " (" + agent.email + ", " + agent.phone + ") — exp " + exp);
        addedCount++;
      } else {
        Logger.log("  " + agent.raw + " (lic " + agent.license + "): exp " + exp + ", does not need renewal");
      }
    });

    Logger.log("Added " + addedCount + " new agent(s) needing renewal.");
  }

  Logger.log("Sync complete. Removed: " + rowsToRemove.length + ", checked new 475s: " + newAgents475.length);
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
