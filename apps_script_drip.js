// ============================================
// COLD EMAIL DRIP SENDER — KHA
// Sends from dj@kalerealty.com via Google Sheets
// Paste this into the SAME Apps Script project as the sync code
// ============================================

// --- CONFIG ---
const CONFIG = {
  SENDER_EMAIL: "dj@kalerealty.com",
  SENDER_NAME: "D.J. Paris",
  DAILY_LIMIT: 220,
  BASE_DELAY_MINUTES: 5,
  RANDOM_DELAY_MINUTES: 5,
  SEND_INTERVAL_DAYS: 7,
  SHEET_NAME: "Emails",
  TEMPLATE_FILE: "email_template.md",
  SPREADSHEET_ID: "1GZ37s4TyJyRVLhtYi3Dl8AiB7vLsq5AgWkYp8ol6Cnc",
  MAKE_WEBHOOK_URL: "https://hook.us1.make.com/hcjb6f8m148ftu6hmkh2i18crgnmrtju",
};

// --- GET SPREADSHEET (works in triggers AND manual runs) ---
function getSpreadsheet() {
  return SpreadsheetApp.openById(CONFIG.SPREADSHEET_ID);
}

// --- COLUMN LOOKUP BY HEADER ---
function getColumnMap(sheet) {
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const map = {};
  headers.forEach((h, i) => {
    const key = h.toString().trim().toLowerCase();
    if (key) map[key] = i;
  });
  return map;
}

// --- MAIN SEND FUNCTION ---
function sendNextEmail() {
  const ss = getSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.SHEET_NAME);
  const props = PropertiesService.getScriptProperties();

  // Check daily send count
  const today = new Date().toDateString();
  const dailyKey = "sent_" + today;
  const sentToday = parseInt(props.getProperty(dailyKey) || "0");
  if (sentToday >= CONFIG.DAILY_LIMIT) {
    Logger.log("Daily limit reached: " + sentToday);
    return;
  }

  // Get template
  const template = getEmailTemplate();
  if (!template) {
    Logger.log("ERROR: Could not find template file: " + CONFIG.TEMPLATE_FILE);
    return;
  }

  // Get column positions from headers
  const colMap = getColumnMap(sheet);
  const firstNameCol = colMap["first name"];
  const emailCol = colMap["email"];
  const statusCol = colMap["status"];
  const timestampCol = colMap["timestamp"];

  if (firstNameCol === undefined || emailCol === undefined || statusCol === undefined || timestampCol === undefined) {
    Logger.log("ERROR: Missing required columns. Found: " + JSON.stringify(colMap));
    return;
  }

  // Find next eligible row: never sent OR sent 7+ days ago
  const data = sheet.getDataRange().getValues();
  const now = new Date();
  let targetRow = -1;

  for (let i = 1; i < data.length; i++) {
    const status = data[i][statusCol];
    const lastSent = data[i][timestampCol];

    // Skip errors and invalid
    if (status === "ERROR" || status === "INVALID") continue;

    // Never sent — eligible
    if (!status || status === "") {
      targetRow = i + 1;
      break;
    }

    // Sent before — check if 7+ days ago
    if (status === "SENT" && lastSent) {
      const sentDate = new Date(lastSent);
      const daysSince = (now - sentDate) / (1000 * 60 * 60 * 24);
      if (daysSince >= CONFIG.SEND_INTERVAL_DAYS) {
        targetRow = i + 1;
        break;
      }
    }
  }

  if (targetRow === -1) {
    Logger.log("No eligible recipients right now. All sent within the last " + CONFIG.SEND_INTERVAL_DAYS + " days.");
    return;
  }

  const firstName = data[targetRow - 1][firstNameCol];
  const email = data[targetRow - 1][emailCol];

  if (!email || !email.toString().includes("@")) {
    sheet.getRange(targetRow, statusCol + 1).setValue("INVALID");
    sheet.getRange(targetRow, timestampCol + 1).setValue(new Date());
    Logger.log("Skipped invalid email at row " + targetRow);
    return;
  }

  // Personalize
  const personalizedBody = template.body.replace(/{{first_name}}/gi, firstName);
  const personalizedSubject = template.subject.replace(/{{first_name}}/gi, firstName);

  try {
    GmailApp.sendEmail(email, personalizedSubject, "", {
      htmlBody: personalizedBody.trim().startsWith("<") ? personalizedBody : markdownToHtml(personalizedBody),
      from: CONFIG.SENDER_EMAIL,
      name: template.senderName || CONFIG.SENDER_NAME,
    });

    sheet.getRange(targetRow, statusCol + 1).setValue("SENT");
    sheet.getRange(targetRow, timestampCol + 1).setValue(new Date());
    props.setProperty(dailyKey, (sentToday + 1).toString());

    // Send text via RingCentral/Make.com
    const phone = data[targetRow - 1][colMap["phone"]];
    sendTextViaWebhook(firstName, email, phone);

    Logger.log("Sent to " + email + " (row " + targetRow + ") | Today: " + (sentToday + 1));
  } catch (e) {
    sheet.getRange(targetRow, statusCol + 1).setValue("ERROR");
    sheet.getRange(targetRow, timestampCol + 1).setValue(e.message);
    Logger.log("Error sending to " + email + ": " + e.message);
  }

  // Schedule next with randomized delay
  deleteExistingTriggers("sendNextEmail");
  const delayMs = (CONFIG.BASE_DELAY_MINUTES + Math.random() * CONFIG.RANDOM_DELAY_MINUTES) * 60 * 1000;
  ScriptApp.newTrigger("sendNextEmail")
    .timeBased()
    .after(delayMs)
    .create();
}

// --- GET TEMPLATE FROM .MD FILE IN DRIVE ---
function getEmailTemplate() {
  const files = DriveApp.getFilesByName(CONFIG.TEMPLATE_FILE);
  if (!files.hasNext()) return null;

  const content = files.next().getBlob().getDataAsString();

  const parts = content.split("---");
  let subject = "Hello";
  let senderName = CONFIG.SENDER_NAME;
  let body = content;

  if (parts.length >= 2) {
    const header = parts[0].trim();
    body = parts.slice(1).join("---").trim();

    const subjectMatch = header.match(/subject:\s*(.+)/i);
    if (subjectMatch) subject = subjectMatch[1].trim();

    const nameMatch = header.match(/from_name:\s*(.+)/i);
    if (nameMatch) senderName = nameMatch[1].trim();
  }

  return { subject, senderName, body };
}

// --- MARKDOWN TO HTML (full support) ---
function markdownToHtml(md) {
  const lines = md.split("\n");
  let html = "";
  let inTable = false;
  let inList = false;
  let inOrderedList = false;
  let tableHeaderDone = false;

  for (let i = 0; i < lines.length; i++) {
    let line = lines[i];

    if (line.startsWith("######")) {
      html += "<h6>" + inlineFormat(line.slice(6).trim()) + "</h6>";
      continue;
    } else if (line.startsWith("#####")) {
      html += "<h5>" + inlineFormat(line.slice(5).trim()) + "</h5>";
      continue;
    } else if (line.startsWith("####")) {
      html += "<h4>" + inlineFormat(line.slice(4).trim()) + "</h4>";
      continue;
    } else if (line.startsWith("###")) {
      html += "<h3>" + inlineFormat(line.slice(3).trim()) + "</h3>";
      continue;
    } else if (line.startsWith("##")) {
      html += "<h2>" + inlineFormat(line.slice(2).trim()) + "</h2>";
      continue;
    } else if (line.startsWith("#")) {
      html += "<h1>" + inlineFormat(line.slice(1).trim()) + "</h1>";
      continue;
    }

    if (/^---+$/.test(line.trim())) {
      if (inTable) { inTable = false; tableHeaderDone = false; html += "</table>"; }
      if (inList) { inList = false; html += "</ul>"; }
      if (inOrderedList) { inOrderedList = false; html += "</ol>"; }
      html += "<hr>";
      continue;
    }

    if (line.trim().startsWith("|")) {
      if (/^\|[\s\-:|]+\|$/.test(line.trim())) {
        tableHeaderDone = true;
        continue;
      }

      const cells = line.split("|").filter(c => c.trim() !== "").map(c => inlineFormat(c.trim()));

      if (!inTable) {
        inTable = true;
        tableHeaderDone = false;
        html += '<table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse;width:100%;margin:12px 0;">';
        html += "<tr>" + cells.map(c => '<th style="background:#f2f2f2;text-align:left;padding:8px;">' + c + "</th>").join("") + "</tr>";
        continue;
      }

      const tag = tableHeaderDone ? "td" : "th";
      const style = tag === "th" ? ' style="background:#f2f2f2;text-align:left;padding:8px;"' : ' style="padding:8px;"';
      html += "<tr>" + cells.map(c => "<" + tag + style + ">" + c + "</" + tag + ">").join("") + "</tr>";
      continue;
    } else if (inTable) {
      inTable = false;
      tableHeaderDone = false;
      html += "</table>";
    }

    if (/^(\s*)[-*]\s/.test(line)) {
      if (!inList) { inList = true; html += "<ul>"; }
      if (inOrderedList) { inOrderedList = false; html += "</ol>"; }
      const content = line.replace(/^(\s*)[-*]\s/, "").trim();
      html += "<li>" + inlineFormat(content) + "</li>";
      continue;
    } else if (inList) {
      inList = false;
      html += "</ul>";
    }

    if (/^\s*\d+\.\s/.test(line)) {
      if (!inOrderedList) { inOrderedList = true; html += "<ol>"; }
      if (inList) { inList = false; html += "</ul>"; }
      const content = line.replace(/^\s*\d+\.\s/, "").trim();
      html += "<li>" + inlineFormat(content) + "</li>";
      continue;
    } else if (inOrderedList) {
      inOrderedList = false;
      html += "</ol>";
    }

    if (line.trim() === "") {
      continue;
    }

    html += "<p>" + inlineFormat(line) + "</p>";
  }

  if (inTable) html += "</table>";
  if (inList) html += "</ul>";
  if (inOrderedList) html += "</ol>";

  return html;
}

// --- INLINE FORMATTING ---
function inlineFormat(text) {
  return text
    .replace(/\*\*(.+?)\*\*/g, "<b>$1</b>")
    .replace(/\*(.+?)\*/g, "<i>$1</i>")
    .replace(/\[(.+?)\]\((.+?)\)/g, '<a href="$2">$1</a>');
}

// --- DELETE EXISTING TRIGGERS ---
function deleteExistingTriggers(functionName) {
  ScriptApp.getProjectTriggers().forEach(trigger => {
    if (trigger.getHandlerFunction() === functionName) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}

// --- SEND TEXT VIA MAKE.COM WEBHOOK ---
function sendTextViaWebhook(firstName, email, phone) {
  if (!phone || phone.toString().trim() === "") {
    Logger.log("No phone number for " + email + ", skipping text.");
    return;
  }

  // Strip to digits only, then normalize to 10-digit US number
  var digits = phone.toString().replace(/\D/g, "");
  if (digits.length === 11 && digits.charAt(0) === "1") {
    digits = digits.substring(1); // remove leading country code
  }
  if (digits.length !== 10) {
    Logger.log("Invalid phone number for " + email + ": " + phone + " (" + digits.length + " digits), skipping text.");
    return;
  }

  const payload = {
    firstName: firstName,
    email: email,
    phone: digits,
    message: firstName + ", pls remember to complete CE & renewal by 4/30 - info here -> https://www.kalehuddle.com/post/illinois-real-estate-license-renewal-2026-complete-guide",
    sentAt: new Date().toISOString(),
  };

  try {
    UrlFetchApp.fetch(CONFIG.MAKE_WEBHOOK_URL, {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify(payload),
      muteHttpExceptions: true,
    });
    Logger.log("Text webhook sent for " + email);
  } catch (e) {
    Logger.log("Webhook error for " + email + ": " + e.message);
  }
}

// --- MANUAL CONTROLS ---

function startCampaign() {
  deleteExistingTriggers("sendNextEmail");
  Logger.log("Campaign started.");
  sendNextEmail();
}

function pauseCampaign() {
  deleteExistingTriggers("sendNextEmail");
  Logger.log("Campaign paused. Run startCampaign() to resume.");
}

function checkStatus() {
  const ss = getSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.SHEET_NAME);
  const data = sheet.getDataRange().getValues();
  const colMap = getColumnMap(sheet);
  const props = PropertiesService.getScriptProperties();

  const statusCol = colMap["status"];
  const today = new Date().toDateString();
  const sentToday = parseInt(props.getProperty("sent_" + today) || "0");

  let sent = 0, unsent = 0, errors = 0;
  for (let i = 1; i < data.length; i++) {
    const s = data[i][statusCol];
    if (s === "SENT") sent++;
    else if (s === "ERROR" || s === "INVALID") errors++;
    else unsent++;
  }

  Logger.log("=== CAMPAIGN STATUS ===");
  Logger.log("Sent: " + sent + " | Unsent: " + unsent + " | Errors: " + errors);
  Logger.log("Sent today: " + sentToday + " / " + CONFIG.DAILY_LIMIT);
  Logger.log("Send interval: every " + CONFIG.SEND_INTERVAL_DAYS + " days per person");
  Logger.log("Active triggers: " + ScriptApp.getProjectTriggers().length);
  Logger.log("Column mapping: " + JSON.stringify(colMap));
}

function fullReset() {
  const props = PropertiesService.getScriptProperties();
  props.deleteAllProperties();
  deleteExistingTriggers("sendNextEmail");

  const ss = getSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.SHEET_NAME);
  const colMap = getColumnMap(sheet);
  const statusCol = colMap["status"];
  const timestampCol = colMap["timestamp"];
  const totalRows = sheet.getLastRow();

  if (totalRows > 1 && statusCol !== undefined) {
    sheet.getRange(2, statusCol + 1, totalRows - 1, 1).clearContent();
  }
  if (totalRows > 1 && timestampCol !== undefined) {
    sheet.getRange(2, timestampCol + 1, totalRows - 1, 1).clearContent();
  }

  Logger.log("Full reset complete.");
}

// --- MENU (includes both campaign + sync controls) ---
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

function testWebhookOnly() {
  sendTextViaWebhook("Test", "test@example.com", "1234567890");
  Logger.log("Test webhook fired.");
}
