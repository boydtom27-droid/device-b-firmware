#include <WiFi.h>
#include <WebServer.h>
#include <Preferences.h>
#include <SPI.h>
#include <ESPmDNS.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>
#include <ArduinoJson.h>
#include <time.h>
#include <Update.h>
#include <GxEPD2_3C.h>
#include <Fonts/FreeMonoBold9pt7b.h>
#include <Fonts/FreeMono9pt7b.h>

#define BUILD_VERSION "DEVICE_B_STABLE_MILESTONE_3_GRAPH_OTA_V8_7_GUARDED_DIAGNOSTIC"

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
Preferences prefs;
const char* fallbackApSSID = "TaskDevice";
const char* fallbackApPassword = "tasks123";
bool usingFallbackAP = false;
String activeAddress = "";
String activeNetworkName = "";
bool mdnsActive = false;

const char* relayBaseUrl = "https://device-b-relay.onrender.com";
const char* relayToken = "abc123xyz789";
const char* firmwareVersion = "3.3.0";

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
bool refreshInProgress = false;
bool renderJobQueued = false;
unsigned long targetJobId = 0;
unsigned long lastAckedJobId = 0;
bool timeSynced = false;
bool otaAttemptedThisBoot = false;

// ---- Diagnostics / guardrails ----
String lastHttpPath = "none";
String lastHttpError = "none";
int lastHttpCode = 0;
unsigned long lastHttpAttemptMs = 0;
unsigned long lastHttpSuccessMs = 0;
String lastFailureStage = "none";
String lastFailureCause = "none";
String lastFailureDetail = "none";
unsigned long lastFailureMs = 0;
unsigned long lastMetaSuccessMs = 0;
unsigned long lastJobSuccessMs = 0;
unsigned long lastAckSuccessMs = 0;
unsigned long lastWiFiRetryMs = 0;
unsigned long stateEnteredMs = 0;
bool manualFetchRequested = false;
int opsDropped = 0;
int unknownOpsDropped = 0;
const unsigned long stateTimeoutMs = 60000UL;
const unsigned long wifiRetryIntervalMs = 120000UL;

enum DeviceState { STATE_IDLE, STATE_POLL_META, STATE_FETCH_JOB, STATE_RENDER_JOB, STATE_ACK_JOB, STATE_COOLDOWN };
DeviceState deviceState = STATE_IDLE;

enum OpType : uint8_t { OP_CLEAR = 0, OP_RECT = 1, OP_FILL_RECT = 2, OP_LINE = 3, OP_TEXT = 4, OP_BAR_OUTLINE = 5, OP_BAR_FILL = 6, OP_URGENT_BORDER = 7, OP_REISSUE_BARS = 8, OP_CROSS = 9, OP_PROGRESS_META = 10, OP_SCHEDULE_PROGRESS_META = 11, OP_DOTTED_RECT = 12, OP_URGENT_TAB = 13, OP_DOTTED_LINE = 14 };
enum FontType : uint8_t { FONT_MONO = 0, FONT_BOLD = 1, FONT_SMALL = 2 }; // FONT_SMALL accepted but rendered as mono in this build
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
String currentPageType = "main";

struct ProgressRegion {
  uint8_t kind; // 0 task epoch, 1 schedule minutes
  int16_t x, y, w, h;
  uint32_t a;
  uint32_t b;
};

const int MAX_PROGRESS_REGIONS = 16;
ProgressRegion progressRegions[MAX_PROGRESS_REGIONS];
int progressRegionCount = 0;
unsigned long lastTimedFullRefresh = 0;
const unsigned long timedFullRefreshInterval = 300000UL;


struct RelayMeta {
  unsigned long pageRevision;
  String pageType;
  unsigned long jobId;
  unsigned long refreshRequested;
  unsigned long forceOTA;
  String firmwareVersionFromRelay;
};
RelayMeta latestMeta;

bool tryConnectOneNetwork(const char* ssid, const char* password, unsigned long timeoutMs);
void stopMDNS();
void startFallbackAP();
void connectPreferredOrFallback();
void syncTimeNow();
bool httpGET(String url, String &out);
bool httpPOSTempty(String url);
bool fetchRelayMetaNow(RelayMeta &meta);
bool fetchRenderJobNow(unsigned long jobId);
bool ackCurrentJob(unsigned long jobId);
void runStateMachine();
void handleButtonRefresh();
void updateBootStatusScreen(const String &line1, const String &line2 = "");
void updateDisplayFromRenderOps();
bool checkForOTA(unsigned long forceFlag);
bool performOTA(String url);
void renderCurrentOpsPaged();
uint16_t mapColor(uint8_t colorCode);
float progressFraction(const ProgressRegion &r);
void updateProgressPartials();
bool requestTimedMainRefresh();
void setDeviceState(DeviceState nextState);
void noteFailure(const String &stage, const String &cause, const String &detail = "");
void maintainWiFiConnection();
String deviceStateName(DeviceState st);
String msAgo(unsigned long t);
String htmlEscape(const String &in);
void handleFetchNow();
void handleRetryWiFi();
void handleLocalForceOTA();

bool waitForDisplay() {
  unsigned long start = millis();
  while (digitalRead(BUSY) == HIGH) {
    delay(1);
    if (millis() - start > 15000) {
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
}

void connectPreferredOrFallback() {
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
      return;
    }
  }
  startFallbackAP();
}

void syncTimeNow() {
  if (usingFallbackAP) {
    timeSynced = false;
    return;
  }
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
  setenv("TZ", "GMT0BST,M3.5.0/1,M10.5.0/2", 1);
  tzset();
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


void noteFailure(const String &stage, const String &cause, const String &detail) {
  lastFailureStage = stage;
  lastFailureCause = cause;
  lastFailureDetail = detail;
  lastFailureMs = millis();
  Serial.print("FAIL "); Serial.print(stage); Serial.print(" "); Serial.print(cause); Serial.print(" "); Serial.println(detail);
}

String msAgo(unsigned long t) {
  if (t == 0) return "never";
  unsigned long d = (millis() - t) / 1000UL;
  return String(d) + "s ago";
}

String deviceStateName(DeviceState st) {
  switch (st) {
    case STATE_IDLE: return "IDLE";
    case STATE_POLL_META: return "POLL_META";
    case STATE_FETCH_JOB: return "FETCH_JOB";
    case STATE_RENDER_JOB: return "RENDER_JOB";
    case STATE_ACK_JOB: return "ACK_JOB";
    case STATE_COOLDOWN: return "COOLDOWN";
    default: return "UNKNOWN";
  }
}

String htmlEscape(const String &in) {
  String out;
  out.reserve(in.length() + 8);
  for (size_t i = 0; i < in.length(); i++) {
    char c = in[i];
    if (c == '&') out += "&amp;";
    else if (c == '<') out += "&lt;";
    else if (c == '>') out += "&gt;";
    else if (c == '"') out += "&quot;";
    else out += c;
  }
  return out;
}

void setDeviceState(DeviceState nextState) {
  if (deviceState != nextState) {
    deviceState = nextState;
    stateEnteredMs = millis();
  }
}

bool httpGET(String url, String &out) {
  lastHttpAttemptMs = millis();
  lastHttpPath = url;
  lastHttpCode = -999;
  lastHttpError = "start";
  out = "";

  if (usingFallbackAP) { lastHttpError = "fallback_ap"; noteFailure("http_get", "connection", "fallback_ap"); return false; }
  if (WiFi.status() != WL_CONNECTED) { lastHttpError = "wifi_not_connected"; noteFailure("http_get", "connection", "wifi_not_connected"); return false; }

  WiFiClientSecure client;
  client.setInsecure();
  HTTPClient http;
  http.setTimeout(15000);
  if (!http.begin(client, url)) {
    lastHttpError = "http_begin_fail";
    noteFailure("http_get", "connection", "http_begin_fail");
    return false;
  }
  int code = http.GET();
  lastHttpCode = code;
  if (code != 200) {
    lastHttpError = String("http_") + String(code);
    http.end();
    noteFailure("http_get", "server", lastHttpError);
    return false;
  }
  out = http.getString();
  http.end();
  lastHttpSuccessMs = millis();
  lastHttpError = "none";
  return true;
}

bool httpPOSTempty(String url) {
  lastHttpAttemptMs = millis();
  lastHttpPath = url;
  lastHttpCode = -999;
  lastHttpError = "post_start";

  if (usingFallbackAP) { lastHttpError = "fallback_ap"; noteFailure("http_post", "connection", "fallback_ap"); return false; }
  if (WiFi.status() != WL_CONNECTED) { lastHttpError = "wifi_not_connected"; noteFailure("http_post", "connection", "wifi_not_connected"); return false; }

  WiFiClientSecure client;
  client.setInsecure();
  HTTPClient http;
  http.setTimeout(15000);
  if (!http.begin(client, url)) {
    lastHttpError = "http_begin_fail";
    noteFailure("http_post", "connection", "http_begin_fail");
    return false;
  }
  int code = http.POST("");
  lastHttpCode = code;
  http.end();
  if (code >= 200 && code < 300) {
    lastHttpSuccessMs = millis();
    lastHttpError = "none";
    return true;
  }
  lastHttpError = String("http_") + String(code);
  noteFailure("http_post", "server", lastHttpError);
  return false;
}

bool fetchRelayMetaNow(RelayMeta &meta) {
  String payload;
  String url = String(relayBaseUrl) + "/api/meta?token=" + relayToken;
  if (!httpGET(url, payload)) { noteFailure("meta", "connection", lastHttpError); return false; }
  DynamicJsonDocument doc(2048);
  DeserializationError err = deserializeJson(doc, payload);
  if (err) { noteFailure("meta", "json", err.c_str()); return false; }
  meta.pageRevision = doc["page_revision"] | 0;
  meta.pageType = doc["page_type"] | "main";
  meta.jobId = doc["job_id"] | 0;
  meta.refreshRequested = doc["refresh_requested"] | 0;
  meta.forceOTA = doc["force_ota"] | 0;
  meta.firmwareVersionFromRelay = doc["firmware_version"] | "";
  lastMetaSuccessMs = millis();
  return true;
}

bool fetchRenderJobNow(unsigned long jobId) {
  String payload;
  String url = String(relayBaseUrl) + "/api/render_job?token=" + relayToken + "&job_id=" + String(jobId);
  if (!httpGET(url, payload)) { noteFailure("render_job", "connection", lastHttpError); return false; }
  DynamicJsonDocument doc(65536);
  DeserializationError err = deserializeJson(doc, payload);
  if (err) { noteFailure("render_job", "json", err.c_str()); return false; }
  if (!(doc["ok"] | false)) { noteFailure("render_job", "server", "ok_false"); return false; }
  currentPageType = doc["page_type"].as<String>();
  JsonArray ops = doc["payload"]["ops"].as<JsonArray>();
  if (ops.isNull()) { noteFailure("render_job", "json", "ops_missing"); return false; }
  renderOpCount = 0;
  progressRegionCount = 0;
  opsDropped = 0;
  unknownOpsDropped = 0;
  for (JsonObject op : ops) {
    String opName = op["op"] | "";

    if (opName == "progress_meta") {
      if (progressRegionCount < MAX_PROGRESS_REGIONS) {
        ProgressRegion &pr = progressRegions[progressRegionCount++];
        pr.kind = 0;
        pr.x = op["x"] | 0; pr.y = op["y"] | 0; pr.w = op["w"] | 0; pr.h = op["h"] | 0;
        pr.a = op["created"] | 0; pr.b = op["deadline"] | 0;
      }
      continue;
    }

    if (opName == "schedule_progress_meta") {
      if (progressRegionCount < MAX_PROGRESS_REGIONS) {
        ProgressRegion &pr = progressRegions[progressRegionCount++];
        pr.kind = 1;
        pr.x = op["x"] | 0; pr.y = op["y"] | 0; pr.w = op["w"] | 0; pr.h = op["h"] | 0;
        pr.a = op["gap_start"] | 0; pr.b = op["gap_end"] | 0;
      }
      continue;
    }

    if (renderOpCount >= MAX_OPS) { opsDropped++; continue; }
    RenderOp &ro = renderOps[renderOpCount];
    ro.x = op["x"] | 0; ro.y = op["y"] | 0; ro.x2 = op["x1"] | op["x2"] | 0; ro.y2 = op["y1"] | op["y2"] | 0;
    ro.w = op["w"] | 0; ro.h = op["h"] | 0; ro.value = op["count"] | 0;
    String color = op["color"] | "black"; ro.color = (color == "red") ? COLOR_RED : ((color == "white") ? COLOR_WHITE : COLOR_BLACK);
    String font = op["font"] | "mono"; ro.font = (font == "bold") ? FONT_BOLD : ((font == "small") ? FONT_SMALL : FONT_MONO);
    const char* textVal = op["text"] | ""; strlcpy(ro.text, textVal, sizeof(ro.text));
    if (opName == "clear") ro.type = OP_CLEAR;
    else if (opName == "rect") ro.type = OP_RECT;
    else if (opName == "fill_rect") ro.type = OP_FILL_RECT;
    else if (opName == "line") { ro.type = OP_LINE; ro.x = op["x1"] | 0; ro.y = op["y1"] | 0; ro.x2 = op["x2"] | 0; ro.y2 = op["y2"] | 0; }
    else if (opName == "text") ro.type = OP_TEXT;
    else if (opName == "bar_outline") ro.type = OP_BAR_OUTLINE;
    else if (opName == "bar_fill") ro.type = OP_BAR_FILL;
    else if (opName == "urgent_border") ro.type = OP_URGENT_BORDER;
    else if (opName == "reissue_bars") ro.type = OP_REISSUE_BARS;
    else if (opName == "cross") ro.type = OP_CROSS;
    else if (opName == "dotted_rect") ro.type = OP_DOTTED_RECT;
    else if (opName == "urgent_tab") ro.type = OP_URGENT_TAB;
    else if (opName == "dotted_line") { ro.type = OP_DOTTED_LINE; ro.x = op["x1"] | 0; ro.y = op["y1"] | 0; ro.x2 = op["x2"] | 0; ro.y2 = op["y2"] | 0; }
    else { unknownOpsDropped++; continue; }
    renderOpCount++;
  }
  lastJobSuccessMs = millis();
  if (opsDropped > 0) noteFailure("render_job", "memory", String("ops_dropped=") + String(opsDropped));
  return true;
}

bool ackCurrentJob(unsigned long jobId) {
  String url = String(relayBaseUrl) + "/api/ack_job?token=" + relayToken + "&job_id=" + String(jobId);
  bool ok = httpPOSTempty(url);
  if (ok) lastAckSuccessMs = millis();
  else noteFailure("ack", "connection", lastHttpError);
  return ok;
}

bool performOTA(String url) {
  if (WiFi.status() != WL_CONNECTED) return false;
  updateBootStatusScreen("OTA update", "Downloading...");
  WiFiClientSecure client; client.setInsecure();
  HTTPClient http;
  if (!http.begin(client, url)) { noteFailure("ota", "connection", "http_begin_fail"); return false; }
  int code = http.GET();
  if (code != 200) { http.end(); noteFailure("ota", "server", String("http_") + String(code)); return false; }
  int contentLength = http.getSize();
  if (contentLength <= 0) { http.end(); noteFailure("ota", "server", "bad_length"); return false; }
  WiFiClient *stream = http.getStreamPtr();
  if (!Update.begin(contentLength)) { http.end(); noteFailure("ota", "memory", "update_begin_fail"); return false; }
  size_t written = Update.writeStream(*stream);
  bool ok = (written == (size_t)contentLength) && Update.end() && Update.isFinished();
  http.end();
  if (!ok) { updateBootStatusScreen("OTA failed", "Update write error"); return false; }
  httpPOSTempty(String(relayBaseUrl) + "/api/ack_ota?token=" + relayToken);
  updateBootStatusScreen("OTA complete", "Rebooting");
  delay(1000);
  ESP.restart();
  return true;
}

bool checkForOTA(unsigned long forceFlag) {
  if (forceFlag == 0) return false;       // manual-only OTA
  if (otaAttemptedThisBoot) return false; // one OTA attempt per boot
  if (usingFallbackAP) return false;
  if (WiFi.status() != WL_CONNECTED) return false;

  String payload;
  String url = String(relayBaseUrl) + "/api/firmware_meta?token=" + relayToken;
  if (!httpGET(url, payload)) return false;

  DynamicJsonDocument doc(1024);
  if (deserializeJson(doc, payload)) return false;

  String newVersion = doc["version"] | "";
  String binUrl = doc["url"] | "";
  if (binUrl.length() == 0) return false;

  otaAttemptedThisBoot = true;

  Serial.print("MANUAL OTA START: ");
  Serial.println(newVersion);

  return performOTA(binUrl);
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

void drawDottedLine(int x1, int y1, int x2, int y2, uint16_t color) {
  const int dash = 4;
  const int gap = 4;
  if (y1 == y2) {
    int dir = (x2 >= x1) ? 1 : -1;
    for (int x = x1; (dir > 0) ? (x < x2) : (x > x2); x += dir * (dash + gap)) {
      int xe = x + dir * dash;
      if ((dir > 0 && xe > x2) || (dir < 0 && xe < x2)) xe = x2;
      display.drawLine(x, y1, xe, y2, color);
    }
  } else if (x1 == x2) {
    int dir = (y2 >= y1) ? 1 : -1;
    for (int y = y1; (dir > 0) ? (y < y2) : (y > y2); y += dir * (dash + gap)) {
      int ye = y + dir * dash;
      if ((dir > 0 && ye > y2) || (dir < 0 && ye < y2)) ye = y2;
      display.drawLine(x1, y, x2, ye, color);
    }
  } else {
    display.drawLine(x1, y1, x2, y2, color);
  }
}

void drawDottedRectOp(const RenderOp &ro) {
  uint16_t c = mapColor(ro.color);
  drawDottedLine(ro.x, ro.y, ro.x + ro.w, ro.y, c);
  drawDottedLine(ro.x, ro.y + ro.h, ro.x + ro.w, ro.y + ro.h, c);
  drawDottedLine(ro.x, ro.y, ro.x, ro.y + ro.h, c);
  drawDottedLine(ro.x + ro.w, ro.y, ro.x + ro.w, ro.y + ro.h, c);
}

void drawUrgentTabOp(const RenderOp &ro) {
  uint16_t c = mapColor(ro.color);
  int size = ro.value > 0 ? ro.value : 14;
  if (size < 8) size = 8;
  for (int i = 0; i < size; i++) {
    display.drawLine(ro.x + ro.w - size + i, ro.y + i, ro.x + ro.w, ro.y + i, c);
  }
}

void executeRenderOpsOnce() {
  for (int i = 0; i < renderOpCount; i++) {
    RenderOp &ro = renderOps[i];
    switch (ro.type) {
      case OP_CLEAR: display.fillScreen(mapColor(ro.color)); break;
      case OP_RECT: display.drawRect(ro.x, ro.y, ro.w, ro.h, mapColor(ro.color)); break;
      case OP_FILL_RECT: display.fillRect(ro.x, ro.y, ro.w, ro.h, mapColor(ro.color)); break;
      case OP_LINE: display.drawLine(ro.x, ro.y, ro.x2, ro.y2, mapColor(ro.color)); break;
      case OP_TEXT:
        applyFont(ro.font); display.setTextColor(mapColor(ro.color)); display.setCursor(ro.x, ro.y); display.print(ro.text); display.setTextColor(GxEPD_BLACK); break;
      case OP_BAR_OUTLINE: display.drawRect(ro.x, ro.y, ro.w, ro.h, mapColor(ro.color)); break;
      case OP_BAR_FILL: if (ro.w > 0 && ro.h > 0) display.fillRect(ro.x, ro.y, ro.w, ro.h, mapColor(ro.color)); break;
      case OP_URGENT_BORDER:
        display.drawRect(ro.x, ro.y, ro.w, ro.h, GxEPD_RED); display.drawRect(ro.x + 1, ro.y + 1, ro.w - 2, ro.h - 2, GxEPD_RED); break;
      case OP_REISSUE_BARS: {
        int segments = 5; int count = ro.value; if (count > segments) count = segments; if (count < 0) count = 0;
        for (int s = 0; s < segments; s++) { int sy = ro.y + (segments - s - 1) * 8; if (s < count) display.fillRect(ro.x, sy, 6, 6, GxEPD_BLACK); else display.drawRect(ro.x, sy, 6, 6, GxEPD_BLACK); }
        break;
      }
      case OP_CROSS:
        display.drawLine(ro.x - 3, ro.y - 3, ro.x + 3, ro.y + 3, mapColor(ro.color));
        display.drawLine(ro.x - 3, ro.y + 3, ro.x + 3, ro.y - 3, mapColor(ro.color));
        break;
      case OP_DOTTED_RECT:
        drawDottedRectOp(ro);
        break;
      case OP_URGENT_TAB:
        drawUrgentTabOp(ro);
        break;
      case OP_DOTTED_LINE:
        drawDottedLine(ro.x, ro.y, ro.x2, ro.y2, mapColor(ro.color));
        break;
      default: break;
    }
    if ((i % 12) == 0) delay(1);
  }
}

void renderCurrentOpsPaged() {
  display.setFullWindow();
  display.firstPage();
  do { executeRenderOpsOnce(); } while (display.nextPage());
}

float progressFraction(const ProgressRegion &r) {
  if (!timeSynced) return -1.0f;
  if (r.kind == 0) {
    if (r.b <= r.a) return 1.0f;
    time_t nowT; time(&nowT);
    float frac = (float)(nowT - (time_t)r.a) / (float)((time_t)r.b - (time_t)r.a);
    if (frac < 0.0f) frac = 0.0f;
    if (frac > 1.0f) frac = 1.0f;
    return frac;
  }
  int gapStart = (int)r.a;
  int gapEnd = (int)r.b;
  if (gapEnd <= gapStart) return 1.0f;
  time_t nowT; time(&nowT);
  struct tm *tmNow = localtime(&nowT);
  if (!tmNow) return -1.0f;
  int nowMins = tmNow->tm_hour * 60 + tmNow->tm_min;
  float frac = (float)(nowMins - gapStart) / (float)(gapEnd - gapStart);
  if (frac < 0.0f) frac = 0.0f;
  if (frac > 1.0f) frac = 1.0f;
  return frac;
}

void updateProgressPartials() {
  if (refreshInProgress) return;
  if (currentPageType != "main") return;
  if (progressRegionCount <= 0) return;
  if (!timeSynced) return;

  refreshInProgress = true;
  displayWake();
  display.init();
  delay(40);
  waitForDisplay();

  for (int i = 0; i < progressRegionCount; i++) {
    const ProgressRegion &pr = progressRegions[i];
    float frac = progressFraction(pr);
    if (frac < 0.0f) continue;
    int fill = (int)(frac * pr.w);
    if (fill < 0) fill = 0;
    if (fill > pr.w) fill = pr.w;
    display.setPartialWindow(pr.x, pr.y, pr.w, pr.h);
    display.firstPage();
    do {
      if (fill > 0) display.fillRect(pr.x, pr.y, fill, pr.h, GxEPD_BLACK);
    } while (display.nextPage());
    delay(1);
  }

  displaySleep();
  refreshInProgress = false;
}

void updateDisplayFromRenderOps() {
  if (refreshInProgress) { noteFailure("display", "busy", "refresh_in_progress"); return; }
  refreshInProgress = true;
  displayWake();
  display.init();
  delay(100);
  if (!waitForDisplay()) { noteFailure("display", "busy", "busy_timeout_before_render"); }
  renderCurrentOpsPaged();
  displaySleep();
  lastTimedFullRefresh = millis();
  refreshInProgress = false;
}

void updateBootStatusScreen(const String &line1, const String &line2) {
  refreshInProgress = true;
  displayWake(); display.init(); delay(100); waitForDisplay();
  display.setFullWindow();
  display.firstPage();
  do {
    display.fillScreen(GxEPD_WHITE);
    display.setTextColor(GxEPD_BLACK);
    display.setFont(&FreeMonoBold9pt7b);
    display.setCursor(20, 60); display.print(line1);
    display.setFont(&FreeMono9pt7b);
    if (line2.length() > 0) { display.setCursor(20, 100); display.print(line2); }
    display.setCursor(20, 150); display.print("Mode: "); display.print(usingFallbackAP ? "Fallback AP" : "Preferred WiFi");
    display.setCursor(20, 180); display.print("Addr: "); display.print(activeAddress);
  } while (display.nextPage());
  displaySleep(); refreshInProgress = false;
}

String localWebpage() {
  String page = "<html><head><meta name='viewport' content='width=device-width,initial-scale=1'>";
  page += "<style>body{font-family:sans-serif;margin:14px;} table{border-collapse:collapse;}td{border:1px solid #ccc;padding:4px 6px;vertical-align:top}.bad{color:#b00;font-weight:bold}.ok{color:#070;font-weight:bold} button{padding:10px 12px;margin:4px;}</style></head><body>";
  page += "<h2>"; page += BUILD_VERSION; page += "</h2>";
  page += "<p>Mode: <b>"; page += usingFallbackAP ? "Fallback AP" : "Preferred WiFi"; page += "</b><br>Network: "; page += activeNetworkName;
  page += "<br>Address: "; page += activeAddress;
  if (mdnsActive) page += "<br>mDNS: taskdevice.local";
  page += "<br>Current page: "; page += currentPageType;
  page += "<br>State: "; page += deviceStateName(deviceState);
  page += "</p>";
  page += "<form action='/refresh'><button>Refresh Device</button></form>";
  page += "<form action='/fetch_now'><button>Fetch Relay Now</button></form>";
  page += "<form action='/retry_wifi'><button>Retry WiFi</button></form>";
  page += "<form action='/force_ota_local' onsubmit=\"return confirm('Start OTA from relay firmware URL?')\"><button>Local Force OTA</button></form>";
  page += "<h3>Diagnostics</h3><table>";
  page += "<tr><td>Last failure</td><td class='bad'>" + htmlEscape(lastFailureStage + " / " + lastFailureCause + " / " + lastFailureDetail) + " (" + msAgo(lastFailureMs) + ")</td></tr>";
  page += "<tr><td>HTTP</td><td>code=" + String(lastHttpCode) + " error=" + htmlEscape(lastHttpError) + " success=" + msAgo(lastHttpSuccessMs) + " attempt=" + msAgo(lastHttpAttemptMs) + "</td></tr>";
  page += "<tr><td>Last URL</td><td>" + htmlEscape(lastHttpPath) + "</td></tr>";
  page += "<tr><td>Meta/job/ack</td><td>meta " + msAgo(lastMetaSuccessMs) + ", job " + msAgo(lastJobSuccessMs) + ", ack " + msAgo(lastAckSuccessMs) + "</td></tr>";
  page += "<tr><td>Job IDs</td><td>target=" + String(targetJobId) + " acked=" + String(lastAckedJobId) + " queued=" + String(renderJobQueued ? "yes" : "no") + "</td></tr>";
  page += "<tr><td>Ops</td><td>loaded=" + String(renderOpCount) + " dropped=" + String(opsDropped) + " unknown=" + String(unknownOpsDropped) + " progress_regions=" + String(progressRegionCount) + "</td></tr>";
  page += "<tr><td>Memory</td><td>heap=" + String(ESP.getFreeHeap()) + " min_heap=" + String(ESP.getMinFreeHeap()) + " psram=" + String(ESP.getFreePsram()) + "</td></tr>";
  page += "<tr><td>WiFi</td><td>status=" + String((int)WiFi.status()) + " RSSI=" + String(WiFi.RSSI()) + " reconnect=" + msAgo(lastWiFiRetryMs) + "</td></tr>";
  page += "</table></body></html>";
  return page;
}

void handleRoot() { server.send(200, "text/html", localWebpage()); }
void handleRefresh() { if (!refreshInProgress) { manualFetchRequested = true; renderJobQueued = true; } server.sendHeader("Location", "/"); server.send(303); }
void handleFetchNow() { if (!refreshInProgress) manualFetchRequested = true; server.sendHeader("Location", "/"); server.send(303); }
void handleRetryWiFi() { if (!refreshInProgress) { connectPreferredOrFallback(); syncTimeNow(); manualFetchRequested = true; } server.sendHeader("Location", "/"); server.send(303); }
void handleLocalForceOTA() { if (!refreshInProgress) { checkForOTA(1); } server.sendHeader("Location", "/"); server.send(303); }

bool requestTimedMainRefresh() {
  if (usingFallbackAP) return false;
  if (WiFi.status() != WL_CONNECTED) return false;
  if (currentPageType != "main") return false;
  String rebuildUrl = String(relayBaseUrl) + "/api/rebuild_now?token=" + relayToken;
  if (!httpPOSTempty(rebuildUrl)) { noteFailure("timed_rebuild", "connection", lastHttpError); return false; }
  if (!fetchRelayMetaNow(latestMeta)) return false;
  if (latestMeta.jobId == 0) { noteFailure("timed_rebuild", "server", "job_id_zero"); return false; }
  targetJobId = latestMeta.jobId;
  renderJobQueued = true;
  lastMetaPoll = millis();
  lastTimedFullRefresh = millis();
  return true;
}

void handleButtonRefresh() {
  static unsigned long lastButtonAction = 0;
  if (digitalRead(REFRESH_BUTTON) == LOW) {
    if (millis() - lastButtonAction > 1200) {
      manualFetchRequested = true;
      renderJobQueued = true;
      lastButtonAction = millis();
    }
  }
}

void runStateMachine() {
  if (deviceState != STATE_IDLE && millis() - stateEnteredMs > stateTimeoutMs) {
    noteFailure("state", "timeout", deviceStateName(deviceState));
    renderJobQueued = false;
    setDeviceState(STATE_IDLE);
  }

  switch (deviceState) {
    case STATE_IDLE:
      if (manualFetchRequested) {
        manualFetchRequested = false;
        setDeviceState(STATE_POLL_META);
      } else if (millis() - lastMetaPoll > metaPollInterval) {
        setDeviceState(STATE_POLL_META);
      } else if (renderJobQueued && targetJobId > 0 && !refreshInProgress) {
        setDeviceState(STATE_FETCH_JOB);
      }
      break;

    case STATE_POLL_META:
      if (fetchRelayMetaNow(latestMeta)) {
        Serial.println("POLL META OK");
        if (latestMeta.jobId > lastAckedJobId || latestMeta.refreshRequested || renderJobQueued) {
          targetJobId = latestMeta.jobId;
          if (targetJobId > 0) renderJobQueued = true;
        }
        if (latestMeta.forceOTA) checkForOTA(1);
      } else {
        Serial.println("POLL META FAIL");
      }
      lastMetaPoll = millis();
      setDeviceState(STATE_IDLE);
      break;

    case STATE_FETCH_JOB:
      if (targetJobId > 0 && fetchRenderJobNow(targetJobId)) {
        Serial.print("FETCH JOB OK: "); Serial.println(targetJobId);
        setDeviceState(STATE_RENDER_JOB);
      } else {
        Serial.println("FETCH JOB FAIL");
        renderJobQueued = false; // avoid repeated tight failure loop; next poll/local refresh will retry
        setDeviceState(STATE_IDLE);
      }
      break;

    case STATE_RENDER_JOB:
      if (!refreshInProgress) {
        updateDisplayFromRenderOps();
        setDeviceState(STATE_ACK_JOB);
      } else {
        noteFailure("render", "busy", "refresh_in_progress");
        setDeviceState(STATE_IDLE);
      }
      break;

    case STATE_ACK_JOB:
      if (ackCurrentJob(targetJobId)) {
        lastAckedJobId = targetJobId;
        renderJobQueued = false;
        Serial.print("ACK JOB OK: "); Serial.println(targetJobId);
      } else {
        Serial.println("ACK JOB FAIL");
      }
      setDeviceState(STATE_COOLDOWN);
      break;

    case STATE_COOLDOWN:
      delay(100);
      setDeviceState(STATE_IDLE);
      break;
  }
}

void maintainWiFiConnection() {
  if (refreshInProgress) return;
  if (usingFallbackAP) {
    if (millis() - lastWiFiRetryMs > wifiRetryIntervalMs) {
      lastWiFiRetryMs = millis();
      connectPreferredOrFallback();
      if (!usingFallbackAP) { syncTimeNow(); manualFetchRequested = true; }
    }
    return;
  }
  if (WiFi.status() != WL_CONNECTED && millis() - lastWiFiRetryMs > 30000UL) {
    noteFailure("wifi", "connection", "sta_disconnected");
    lastWiFiRetryMs = millis();
    connectPreferredOrFallback();
    if (!usingFallbackAP) { syncTimeNow(); manualFetchRequested = true; }
  }
}

void setup() {
  otaAttemptedThisBoot = false;

  Serial.begin(115200);
  delay(400);
  Serial.println();
  Serial.println("BOOT: DEVICE_B_STABLE_MILESTONE_3_GRAPH_OTA_V8_CALENDAR_ORDER_REISSUE");
  pinMode(PWR_PIN, OUTPUT); digitalWrite(PWR_PIN, LOW); pinMode(REFRESH_BUTTON, INPUT_PULLUP);
  connectPreferredOrFallback();
  updateBootStatusScreen("Booting...", activeAddress);
  Serial.print("WIFI MODE: "); Serial.println(usingFallbackAP ? "Fallback AP" : "Preferred WiFi");
  Serial.print("ADDRESS: "); Serial.println(activeAddress);
  syncTimeNow();
  server.on("/", handleRoot); server.on("/refresh", handleRefresh); server.on("/fetch_now", handleFetchNow); server.on("/retry_wifi", handleRetryWiFi); server.on("/force_ota_local", handleLocalForceOTA); server.begin();
  Serial.println("WEB SERVER STARTED");
  if (fetchRelayMetaNow(latestMeta)) {
    Serial.println("META OK");
    targetJobId = latestMeta.jobId;
    if (targetJobId > 0 && fetchRenderJobNow(targetJobId)) {
      Serial.print("JOB FETCH OK: "); Serial.println(targetJobId);
      updateDisplayFromRenderOps(); ackCurrentJob(targetJobId); lastAckedJobId = targetJobId;
    } else updateBootStatusScreen("Relay online", "No job");
  } else {
    updateBootStatusScreen("Relay fetch fail", lastFailureCause + ":" + lastFailureDetail);
  }
  lastMetaPoll = millis() - metaPollInterval + 5000UL;
  lastTimedFullRefresh = millis();
  setDeviceState(STATE_IDLE);
}

void loop() {
  server.handleClient();
  maintainWiFiConnection();
  if (!usingFallbackAP && (millis() - lastTimeSync > timeSyncInterval)) syncTimeNow();
  handleButtonRefresh();
  runStateMachine();
  if (!refreshInProgress && (millis() - lastTimedFullRefresh > timedFullRefreshInterval)) {
    requestTimedMainRefresh();
  }
  delay(1);
}
