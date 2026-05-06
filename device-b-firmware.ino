#include <WiFi.h>
#include <WebServer.h>
#include <SPI.h>
#include <ESPmDNS.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>
#include <ArduinoJson.h>
#include <time.h>
#include <GxEPD2_3C.h>
#include <Fonts/FreeMonoBold9pt7b.h>
#include <Fonts/FreeMono9pt7b.h>

#define BUILD_VERSION "DEVICE_B_STABLE_CORE_MODERN_RENDER_V1_NO_OTA"

#define CS 10
#define DC 9
#define RST 8
#define BUSY 7
#define PWR_PIN 21
#define REFRESH_BUTTON 14

GxEPD2_3C<GxEPD2_750c_Z08, GxEPD2_750c_Z08::HEIGHT> display(
  GxEPD2_750c_Z08(CS, DC, RST, BUSY)
);

WebServer server(80);

const char* fallbackApSSID = "TaskDevice";
const char* fallbackApPassword = "tasks123";
bool usingFallbackAP = false;
String activeAddress = "";
String activeNetworkName = "";
bool mdnsActive = false;

const char* relayBaseUrl = "https://device-b-relay.onrender.com";
const char* relayToken = "abc123xyz789";
const char* firmwareVersion = "stable_core_modern_render_v1";

struct SavedNetwork {
  const char* ssid;
  const char* password;
};

SavedNetwork preferredNetworks[] = {
  {"ASUS", "le0pardess"},
  {"guest-dog", "givemeinternet"},
  {"Tomspot", "Tom00001"}
};
const int preferredNetworkCount = sizeof(preferredNetworks) / sizeof(preferredNetworks[0]);

unsigned long lastMetaPoll = 0;
const unsigned long metaPollInterval = 30000UL;
unsigned long lastTimeSync = 0;
const unsigned long timeSyncInterval = 21600000UL;
unsigned long lastWifiRetry = 0;
const unsigned long wifiRetryInterval = 60000UL;
unsigned long lastTimedMainRefresh = 0;
const unsigned long timedMainRefreshInterval = 300000UL;

bool refreshInProgress = false;
bool renderJobQueued = false;
unsigned long targetJobId = 0;
unsigned long lastAckedJobId = 0;
bool timeSynced = false;

String currentPageType = "main";

String lastFaultStage = "none";
String lastFaultDetail = "";
int lastHttpCode = 0;
String lastHttpUrl = "";
unsigned long lastSuccessfulMetaMillis = 0;
unsigned long lastSuccessfulJobMillis = 0;
unsigned long lastSuccessfulAckMillis = 0;
unsigned long lastRenderMillis = 0;
int opsDropped = 0;

struct RelayMeta {
  unsigned long pageRevision;
  String pageType;
  unsigned long jobId;
  unsigned long refreshRequested;
  unsigned long forceOTA;
  String firmwareVersionFromRelay;
};
RelayMeta latestMeta;

enum DeviceState { STATE_IDLE, STATE_POLL_META, STATE_FETCH_JOB, STATE_RENDER_JOB, STATE_ACK_JOB, STATE_COOLDOWN };
DeviceState deviceState = STATE_IDLE;

enum OpType : uint8_t {
  OP_CLEAR = 0,
  OP_RECT = 1,
  OP_FILL_RECT = 2,
  OP_LINE = 3,
  OP_TEXT = 4,
  OP_BAR_OUTLINE = 5,
  OP_BAR_FILL = 6,
  OP_URGENT_BORDER = 7,
  OP_REISSUE_BARS = 8,
  OP_CROSS = 9,
  OP_DOTTED_RECT = 10,
  OP_URGENT_TAB = 11,
  OP_DOTTED_LINE = 12
};

enum FontType : uint8_t { FONT_MONO = 0, FONT_BOLD = 1 };
enum ColorType : uint8_t { COLOR_BLACK = 0, COLOR_RED = 1, COLOR_WHITE = 2 };

struct RenderOp {
  uint8_t type;
  int16_t x;
  int16_t y;
  int16_t x2;
  int16_t y2;
  int16_t w;
  int16_t h;
  uint8_t color;
  uint8_t font;
  int16_t value;
  char text[96];
};

const int MAX_OPS = 420;
RenderOp renderOps[MAX_OPS];
int renderOpCount = 0;

void setFault(const String &stage, const String &detail) {
  lastFaultStage = stage;
  lastFaultDetail = detail;
  Serial.print("FAULT ");
  Serial.print(stage);
  Serial.print(": ");
  Serial.println(detail);
}

uint16_t mapColor(uint8_t colorCode) {
  if (colorCode == COLOR_RED) return GxEPD_RED;
  if (colorCode == COLOR_WHITE) return GxEPD_WHITE;
  return GxEPD_BLACK;
}

void applyFont(uint8_t fontCode) {
  if (fontCode == FONT_BOLD) display.setFont(&FreeMonoBold9pt7b);
  else display.setFont(&FreeMono9pt7b);
}

bool waitForDisplay() {
  unsigned long start = millis();
  while (digitalRead(BUSY) == HIGH) {
    delay(1);
    if (millis() - start > 15000) {
      setFault("display", "BUSY_TIMEOUT");
      Serial.println("Busy Timeout!");
      display.end();
      delay(500);
      display.init();
      return false;
    }
  }
  return true;
}

void displayWake() {
  pinMode(PWR_PIN, OUTPUT);
  digitalWrite(PWR_PIN, HIGH);
  delay(80);
}

void displaySleep() {
  display.hibernate();
  delay(50);
  digitalWrite(PWR_PIN, LOW);
}

bool tryConnectOneNetwork(const char* ssid, const char* password, unsigned long timeoutMs) {
  if (ssid == nullptr || strlen(ssid) == 0) return false;
  Serial.print("Trying WiFi: ");
  Serial.println(ssid);
  WiFi.begin(ssid, password);
  unsigned long start = millis();
  while (millis() - start < timeoutMs) {
    if (WiFi.status() == WL_CONNECTED) return true;
    delay(250);
  }
  WiFi.disconnect(true, true);
  delay(300);
  return false;
}

void stopMDNS() {
  if (mdnsActive) {
    MDNS.end();
    mdnsActive = false;
  }
}

void startFallbackAP() {
  stopMDNS();
  WiFi.mode(WIFI_AP);
  WiFi.softAP(fallbackApSSID, fallbackApPassword);
  usingFallbackAP = true;
  activeNetworkName = "AP";
  activeAddress = WiFi.softAPIP().toString();
  setFault("connection", "fallback_ap");
}

void connectPreferredOrFallback() {
  stopMDNS();
  WiFi.mode(WIFI_STA);
  WiFi.disconnect(true, true);
  delay(300);
  for (int i = 0; i < preferredNetworkCount; i++) {
    if (tryConnectOneNetwork(preferredNetworks[i].ssid, preferredNetworks[i].password, 8000)) {
      usingFallbackAP = false;
      activeNetworkName = preferredNetworks[i].ssid;
      activeAddress = WiFi.localIP().toString();
      if (MDNS.begin("taskdevice")) {
        MDNS.addService("http", "tcp", 80);
        mdnsActive = true;
      }
      setFault("none", "");
      return;
    }
  }
  startFallbackAP();
}

void reconnectPreferredIfNeeded(bool force = false) {
  if (!force && (millis() - lastWifiRetry < wifiRetryInterval)) return;
  if (!force && !usingFallbackAP && WiFi.status() == WL_CONNECTED) return;
  lastWifiRetry = millis();
  Serial.println("WiFi retry requested");
  connectPreferredOrFallback();
}

void syncTimeNow() {
  if (usingFallbackAP || WiFi.status() != WL_CONNECTED) {
    timeSynced = false;
    return;
  }
  setenv("TZ", "GMT0BST,M3.5.0/1,M10.5.0/2", 1);
  tzset();
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
  struct tm timeinfo;
  for (int i = 0; i < 12; i++) {
    if (getLocalTime(&timeinfo)) {
      timeSynced = true;
      lastTimeSync = millis();
      return;
    }
    delay(250);
  }
  timeSynced = false;
}

bool httpGET(String url, String &out) {
  lastHttpUrl = url;
  lastHttpCode = 0;
  Serial.print("HTTP GET: ");
  Serial.println(url);
  if (usingFallbackAP) {
    setFault("connection", "fallback_ap");
    return false;
  }
  if (WiFi.status() != WL_CONNECTED) {
    setFault("connection", "wifi_not_connected");
    return false;
  }

  WiFiClientSecure client;
  client.setInsecure();
  HTTPClient http;
  if (!http.begin(client, url)) {
    setFault("connection", "http_begin_failed");
    return false;
  }
  int code = http.GET();
  lastHttpCode = code;
  Serial.print("HTTP CODE: ");
  Serial.println(code);
  if (code != 200) {
    setFault("connection", String("http_") + String(code));
    http.end();
    return false;
  }
  out = http.getString();
  http.end();
  lastFaultStage = "none";
  lastFaultDetail = "";
  return true;
}

bool httpPOSTempty(String url) {
  lastHttpUrl = url;
  lastHttpCode = 0;
  Serial.print("HTTP POST: ");
  Serial.println(url);
  if (usingFallbackAP) {
    setFault("connection", "fallback_ap");
    return false;
  }
  if (WiFi.status() != WL_CONNECTED) {
    setFault("connection", "wifi_not_connected");
    return false;
  }
  WiFiClientSecure client;
  client.setInsecure();
  HTTPClient http;
  if (!http.begin(client, url)) {
    setFault("connection", "http_begin_failed");
    return false;
  }
  int code = http.POST("");
  lastHttpCode = code;
  Serial.print("HTTP POST CODE: ");
  Serial.println(code);
  http.end();
  if (!(code >= 200 && code < 300)) {
    setFault("connection", String("http_post_") + String(code));
    return false;
  }
  return true;
}

bool fetchRelayMetaNow(RelayMeta &meta) {
  String payload;
  String url = String(relayBaseUrl) + "/api/meta?token=" + relayToken;
  if (!httpGET(url, payload)) return false;
  DynamicJsonDocument doc(2048);
  DeserializationError err = deserializeJson(doc, payload);
  if (err) {
    setFault("json", String("meta_") + err.c_str());
    return false;
  }
  meta.pageRevision = doc["page_revision"] | 0;
  meta.pageType = doc["page_type"] | "main";
  meta.jobId = doc["job_id"] | 0;
  meta.refreshRequested = doc["refresh_requested"] | 0;
  meta.forceOTA = doc["force_ota"] | 0;
  meta.firmwareVersionFromRelay = doc["firmware_version"] | "";
  lastSuccessfulMetaMillis = millis();
  return true;
}

void loadCommonOpFields(RenderOp &ro, JsonObject op) {
  ro.x = op["x"] | 0;
  ro.y = op["y"] | 0;
  ro.x2 = op["x2"] | 0;
  ro.y2 = op["y2"] | 0;
  ro.w = op["w"] | 0;
  ro.h = op["h"] | 0;
  ro.value = op["count"] | 0;
  String color = op["color"] | "black";
  ro.color = (color == "red") ? COLOR_RED : ((color == "white") ? COLOR_WHITE : COLOR_BLACK);
  String font = op["font"] | "mono";
  ro.font = (font == "bold") ? FONT_BOLD : FONT_MONO;
  const char* textVal = op["text"] | "";
  strlcpy(ro.text, textVal, sizeof(ro.text));
}

bool fetchRenderJobNow(unsigned long jobId) {
  String payload;
  String url = String(relayBaseUrl) + "/api/render_job?token=" + relayToken + "&job_id=" + String(jobId);
  if (!httpGET(url, payload)) return false;

  DynamicJsonDocument doc(65536);
  DeserializationError err = deserializeJson(doc, payload);
  if (err) {
    setFault("json", String("job_") + err.c_str());
    return false;
  }
  if (!(doc["ok"] | false)) {
    setFault("json", "job_not_ok");
    return false;
  }

  currentPageType = doc["page_type"].as<String>();
  JsonArray ops = doc["payload"]["ops"].as<JsonArray>();
  renderOpCount = 0;
  opsDropped = 0;

  for (JsonObject op : ops) {
    String opName = op["op"] | "";
    if (renderOpCount >= MAX_OPS) {
      opsDropped++;
      continue;
    }

    RenderOp &ro = renderOps[renderOpCount];
    loadCommonOpFields(ro, op);

    if (opName == "clear") ro.type = OP_CLEAR;
    else if (opName == "rect") ro.type = OP_RECT;
    else if (opName == "fill_rect") ro.type = OP_FILL_RECT;
    else if (opName == "line") {
      ro.type = OP_LINE;
      ro.x = op["x1"] | 0;
      ro.y = op["y1"] | 0;
      ro.x2 = op["x2"] | 0;
      ro.y2 = op["y2"] | 0;
    }
    else if (opName == "text") ro.type = OP_TEXT;
    else if (opName == "bar_outline") ro.type = OP_BAR_OUTLINE;
    else if (opName == "bar_fill") ro.type = OP_BAR_FILL;
    else if (opName == "urgent_border") ro.type = OP_URGENT_BORDER;
    else if (opName == "reissue_bars") ro.type = OP_REISSUE_BARS;
    else if (opName == "cross") ro.type = OP_CROSS;
    else if (opName == "dotted_rect") ro.type = OP_DOTTED_RECT;
    else if (opName == "urgent_tab") ro.type = OP_URGENT_TAB;
    else if (opName == "dotted_line") {
      ro.type = OP_DOTTED_LINE;
      ro.x = op["x1"] | 0;
      ro.y = op["y1"] | 0;
      ro.x2 = op["x2"] | 0;
      ro.y2 = op["y2"] | 0;
    }
    else {
      // Unknown ops are ignored safely so relay and firmware can evolve independently.
      continue;
    }
    renderOpCount++;
  }

  if (opsDropped > 0) setFault("render", String("ops_dropped_") + String(opsDropped));
  lastSuccessfulJobMillis = millis();
  Serial.print("OPS LOADED: ");
  Serial.println(renderOpCount);
  return true;
}

bool ackCurrentJob(unsigned long jobId) {
  String url = String(relayBaseUrl) + "/api/ack_job?token=" + relayToken + "&job_id=" + String(jobId);
  bool ok = httpPOSTempty(url);
  if (ok) lastSuccessfulAckMillis = millis();
  return ok;
}

void drawDottedLine(int x1, int y1, int x2, int y2, uint16_t color, int dash = 4, int gap = 4) {
  if (x1 == x2) {
    int yStart = min(y1, y2);
    int yEnd = max(y1, y2);
    for (int y = yStart; y < yEnd; y += dash + gap) {
      display.drawLine(x1, y, x1, min(y + dash, yEnd), color);
    }
  } else if (y1 == y2) {
    int xStart = min(x1, x2);
    int xEnd = max(x1, x2);
    for (int x = xStart; x < xEnd; x += dash + gap) {
      display.drawLine(x, y1, min(x + dash, xEnd), y1, color);
    }
  } else {
    display.drawLine(x1, y1, x2, y2, color);
  }
}

void drawDottedRect(const RenderOp &ro) {
  uint16_t c = mapColor(ro.color);
  drawDottedLine(ro.x, ro.y, ro.x + ro.w, ro.y, c);
  drawDottedLine(ro.x, ro.y + ro.h, ro.x + ro.w, ro.y + ro.h, c);
  drawDottedLine(ro.x, ro.y, ro.x, ro.y + ro.h, c);
  drawDottedLine(ro.x + ro.w, ro.y, ro.x + ro.w, ro.y + ro.h, c);
}

void drawUrgentTab(const RenderOp &ro) {
  int x = ro.x;
  int y = ro.y;
  int s = ro.value > 0 ? ro.value : 10;
  uint16_t c = mapColor(ro.color);
  display.fillTriangle(x, y, x - s, y, x, y + s, c);
}

void executeRenderOpsOnce() {
  for (int i = 0; i < renderOpCount; i++) {
    RenderOp &ro = renderOps[i];
    switch (ro.type) {
      case OP_CLEAR:
        display.fillScreen(mapColor(ro.color));
        break;
      case OP_RECT:
        display.drawRect(ro.x, ro.y, ro.w, ro.h, mapColor(ro.color));
        break;
      case OP_FILL_RECT:
        display.fillRect(ro.x, ro.y, ro.w, ro.h, mapColor(ro.color));
        break;
      case OP_LINE:
        display.drawLine(ro.x, ro.y, ro.x2, ro.y2, mapColor(ro.color));
        break;
      case OP_TEXT:
        applyFont(ro.font);
        display.setTextColor(mapColor(ro.color));
        display.setCursor(ro.x, ro.y);
        display.print(ro.text);
        display.setTextColor(GxEPD_BLACK);
        break;
      case OP_BAR_OUTLINE:
        display.drawRect(ro.x, ro.y, ro.w, ro.h, mapColor(ro.color));
        break;
      case OP_BAR_FILL:
        if (ro.w > 0 && ro.h > 0) display.fillRect(ro.x, ro.y, ro.w, ro.h, mapColor(ro.color));
        break;
      case OP_URGENT_BORDER:
        display.drawRect(ro.x, ro.y, ro.w, ro.h, GxEPD_RED);
        display.drawRect(ro.x + 1, ro.y + 1, ro.w - 2, ro.h - 2, GxEPD_RED);
        break;
      case OP_REISSUE_BARS: {
        int segments = 5;
        int count = ro.value;
        if (count > segments) count = segments;
        if (count < 0) count = 0;
        for (int s = 0; s < segments; s++) {
          int sy = ro.y + (segments - s - 1) * 8;
          if (s < count) display.fillRect(ro.x, sy, 6, 6, GxEPD_BLACK);
          else display.drawRect(ro.x, sy, 6, 6, GxEPD_BLACK);
        }
        break;
      }
      case OP_CROSS:
        display.drawLine(ro.x - 3, ro.y - 3, ro.x + 3, ro.y + 3, mapColor(ro.color));
        display.drawLine(ro.x - 3, ro.y + 3, ro.x + 3, ro.y - 3, mapColor(ro.color));
        break;
      case OP_DOTTED_RECT:
        drawDottedRect(ro);
        break;
      case OP_URGENT_TAB:
        drawUrgentTab(ro);
        break;
      case OP_DOTTED_LINE:
        drawDottedLine(ro.x, ro.y, ro.x2, ro.y2, mapColor(ro.color));
        break;
      default:
        break;
    }
    if ((i % 12) == 0) delay(1);
  }
}

void drawDynamicProgressFillsOnce() {
  // Deliberately empty in this stable build.
  // Dynamic/partial progress refresh experiments are removed.
  // The relay should send static bar_fill and notch ops for full refresh only.
}

void renderCurrentOpsPaged() {
  display.setFullWindow();
  display.firstPage();
  do {
    executeRenderOpsOnce();
    drawDynamicProgressFillsOnce();
  } while (display.nextPage());
}

void updateDisplayFromRenderOps() {
  if (refreshInProgress) return;
  refreshInProgress = true;
  displayWake();
  display.init();
  delay(100);
  waitForDisplay();
  renderCurrentOpsPaged();
  displaySleep();
  lastTimedMainRefresh = millis();
  lastRenderMillis = millis();
  refreshInProgress = false;
}

void updateBootStatusScreen(const String &line1, const String &line2 = "") {
  refreshInProgress = true;
  displayWake();
  display.init();
  delay(100);
  waitForDisplay();
  display.setFullWindow();
  display.firstPage();
  do {
    display.fillScreen(GxEPD_WHITE);
    display.setTextColor(GxEPD_BLACK);
    display.setFont(&FreeMonoBold9pt7b);
    display.setCursor(20, 60);
    display.print(line1);
    display.setFont(&FreeMono9pt7b);
    if (line2.length() > 0) {
      display.setCursor(20, 100);
      display.print(line2);
    }
    display.setCursor(20, 150);
    display.print("Mode: ");
    display.print(usingFallbackAP ? "Fallback AP" : "Preferred WiFi");
    display.setCursor(20, 180);
    display.print("Addr: ");
    display.print(activeAddress);
    if (lastFaultStage != "none") {
      display.setCursor(20, 220);
      display.print("Fault: ");
      display.print(lastFaultStage);
      display.setCursor(20, 250);
      display.print(lastFaultDetail);
    }
  } while (display.nextPage());
  displaySleep();
  refreshInProgress = false;
}

String stateName() {
  switch (deviceState) {
    case STATE_IDLE: return "IDLE";
    case STATE_POLL_META: return "POLL_META";
    case STATE_FETCH_JOB: return "FETCH_JOB";
    case STATE_RENDER_JOB: return "RENDER_JOB";
    case STATE_ACK_JOB: return "ACK_JOB";
    case STATE_COOLDOWN: return "COOLDOWN";
  }
  return "UNKNOWN";
}

String localWebpage() {
  String page = "<html><body><meta name='viewport' content='width=device-width, initial-scale=1'>";
  page += "<h2>"; page += BUILD_VERSION; page += "</h2>";
  page += "<p>Mode: "; page += usingFallbackAP ? "Fallback AP" : "Preferred WiFi";
  page += "<br>Network: "; page += activeNetworkName;
  page += "<br>Address: "; page += activeAddress;
  if (mdnsActive) page += "<br>mDNS: taskdevice.local";
  page += "<br>Current page: "; page += currentPageType;
  page += "<br>State: "; page += stateName();
  page += "<br>Refresh in progress: "; page += refreshInProgress ? "yes" : "no";
  page += "<br>Queued: "; page += renderJobQueued ? "yes" : "no";
  page += "<br>Target job: "; page += String(targetJobId);
  page += "<br>Last acked job: "; page += String(lastAckedJobId);
  page += "</p>";
  page += "<h3>Diagnostics</h3><p>Fault: "; page += lastFaultStage; page += " / "; page += lastFaultDetail;
  page += "<br>HTTP code: "; page += String(lastHttpCode);
  page += "<br>Last URL: "; page += lastHttpUrl;
  page += "<br>WiFi status: "; page += String((int)WiFi.status());
  page += "<br>RSSI: "; page += String(WiFi.status() == WL_CONNECTED ? WiFi.RSSI() : 0);
  page += "<br>Free heap: "; page += String(ESP.getFreeHeap());
  page += "<br>Free PSRAM: "; page += String(ESP.getFreePsram());
  page += "<br>Ops loaded: "; page += String(renderOpCount);
  page += "<br>Ops dropped: "; page += String(opsDropped);
  page += "<br>Last meta ms: "; page += String(lastSuccessfulMetaMillis);
  page += "<br>Last job ms: "; page += String(lastSuccessfulJobMillis);
  page += "<br>Last ack ms: "; page += String(lastSuccessfulAckMillis);
  page += "<br>Last render ms: "; page += String(lastRenderMillis);
  page += "</p>";
  page += "<form action='/refresh'><input type='submit' value='Refresh Device'></form>";
  page += "<form action='/fetch_now'><input type='submit' value='Fetch Relay Now'></form>";
  page += "<form action='/retry_wifi'><input type='submit' value='Retry WiFi'></form>";
  page += "</body></html>";
  return page;
}

void handleRoot() {
  server.send(200, "text/html", localWebpage());
}

void handleRefresh() {
  if (!refreshInProgress) {
    if (targetJobId == 0) deviceState = STATE_POLL_META;
    else renderJobQueued = true;
  }
  server.sendHeader("Location", "/");
  server.send(303);
}

void handleFetchNow() {
  if (!refreshInProgress) deviceState = STATE_POLL_META;
  server.sendHeader("Location", "/");
  server.send(303);
}

void handleRetryWiFi() {
  reconnectPreferredIfNeeded(true);
  server.sendHeader("Location", "/");
  server.send(303);
}

void handleButtonRefresh() {
  static unsigned long lastButtonAction = 0;
  if (digitalRead(REFRESH_BUTTON) == LOW) {
    if (millis() - lastButtonAction > 1200) {
      if (!refreshInProgress) renderJobQueued = true;
      lastButtonAction = millis();
    }
  }
}

void runStateMachine() {
  switch (deviceState) {
    case STATE_IDLE:
      if (millis() - lastMetaPoll > metaPollInterval) deviceState = STATE_POLL_META;
      else if (renderJobQueued && targetJobId > 0 && !refreshInProgress) deviceState = STATE_FETCH_JOB;
      break;

    case STATE_POLL_META:
      if (fetchRelayMetaNow(latestMeta)) {
        Serial.println("POLL META OK");
        if (latestMeta.jobId > lastAckedJobId || latestMeta.refreshRequested) {
          targetJobId = latestMeta.jobId;
          renderJobQueued = true;
        }
        // OTA intentionally removed. force_ota is ignored.
      } else {
        Serial.println("POLL META FAIL");
        if (lastFaultStage == "none") setFault("connection", "meta_fetch_failed");
      }
      lastMetaPoll = millis();
      deviceState = STATE_IDLE;
      break;

    case STATE_FETCH_JOB:
      if (targetJobId > 0 && fetchRenderJobNow(targetJobId)) {
        Serial.print("FETCH JOB OK: ");
        Serial.println(targetJobId);
        deviceState = STATE_RENDER_JOB;
      } else {
        Serial.println("FETCH JOB FAIL");
        if (lastFaultStage == "none") setFault("fetch", "job_fetch_failed");
        renderJobQueued = false;
        deviceState = STATE_IDLE;
      }
      break;

    case STATE_RENDER_JOB:
      if (!refreshInProgress) {
        updateDisplayFromRenderOps();
        deviceState = STATE_ACK_JOB;
      } else {
        deviceState = STATE_IDLE;
      }
      break;

    case STATE_ACK_JOB:
      if (ackCurrentJob(targetJobId)) {
        lastAckedJobId = targetJobId;
        renderJobQueued = false;
        Serial.print("ACK JOB OK: ");
        Serial.println(targetJobId);
      } else {
        Serial.println("ACK JOB FAIL");
        setFault("connection", "ack_failed");
      }
      deviceState = STATE_COOLDOWN;
      break;

    case STATE_COOLDOWN:
      delay(100);
      deviceState = STATE_IDLE;
      break;
  }
}

void setup() {
  Serial.begin(115200);
  delay(400);
  Serial.println();
  Serial.println("BOOT: DEVICE_B_STABLE_CORE_MODERN_RENDER_V1_NO_OTA");

  pinMode(PWR_PIN, OUTPUT);
  digitalWrite(PWR_PIN, LOW);
  pinMode(REFRESH_BUTTON, INPUT_PULLUP);

  connectPreferredOrFallback();
  updateBootStatusScreen("Booting...", activeAddress);
  Serial.print("WIFI MODE: ");
  Serial.println(usingFallbackAP ? "Fallback AP" : "Preferred WiFi");
  Serial.print("ADDRESS: ");
  Serial.println(activeAddress);

  syncTimeNow();

  server.on("/", handleRoot);
  server.on("/refresh", handleRefresh);
  server.on("/fetch_now", handleFetchNow);
  server.on("/retry_wifi", handleRetryWiFi);
  server.begin();
  Serial.println("WEB SERVER STARTED");

  if (fetchRelayMetaNow(latestMeta)) {
    Serial.println("META OK");
    targetJobId = latestMeta.jobId;
    if (targetJobId > 0 && fetchRenderJobNow(targetJobId)) {
      Serial.print("JOB FETCH OK: ");
      Serial.println(targetJobId);
      updateDisplayFromRenderOps();
      ackCurrentJob(targetJobId);
      lastAckedJobId = targetJobId;
    } else {
      updateBootStatusScreen("Relay online", "No job");
    }
  } else {
    updateBootStatusScreen("Relay fetch fail", activeAddress);
  }

  lastMetaPoll = millis();
  lastTimedMainRefresh = millis();
  deviceState = STATE_IDLE;
}

void loop() {
  server.handleClient();

  if (!usingFallbackAP && (millis() - lastTimeSync > timeSyncInterval)) {
    syncTimeNow();
  }

  reconnectPreferredIfNeeded(false);
  handleButtonRefresh();
  runStateMachine();

  if (currentPageType == "main" &&
      !refreshInProgress &&
      !renderJobQueued &&
      (millis() - lastTimedMainRefresh > timedMainRefreshInterval)) {
    Serial.println("TIMED MAIN FULL REFRESH");
    updateDisplayFromRenderOps();
  }

  delay(1);
}
