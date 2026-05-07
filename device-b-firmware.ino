#include <WiFi.h>
#include <WebServer.h>
#include <Preferences.h>
#include <SPI.h>
#include <ESPmDNS.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>
#include <ArduinoJson.h>
#include <time.h>
#include <esp_heap_caps.h>
#include <GxEPD2_3C.h>
#include <Fonts/FreeMonoBold9pt7b.h>
#include <Fonts/FreeMono9pt7b.h>

#define BUILD_VERSION "DEVICE_B_CLEAN_SCHEDULER_V6_CALENDAR_SMALLFONT_NO_OTA"

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
const char* firmwareVersion = "clean_scheduler_v4_https_insecure_psram_diag_no_ota";

// HTTPS is used because Render forces HTTP -> HTTPS.
// This reliability branch deliberately uses WiFiClientSecure::setInsecure(),
// matching the earlier working firmware and avoiding CA-chain failures seen with Render.


struct SavedNetwork {
  const char* ssid;
  const char* password;
};
SavedNetwork preferredNetworks[] = {
  {"VM6269662", "FollyDaRabbit123"},
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

enum DeviceState { STATE_IDLE, STATE_POLL_META, STATE_FETCH_JOB, STATE_RENDER_JOB, STATE_ACK_JOB, STATE_COOLDOWN };
DeviceState deviceState = STATE_IDLE;

enum OpType : uint8_t { OP_CLEAR = 0, OP_RECT = 1, OP_FILL_RECT = 2, OP_LINE = 3, OP_TEXT = 4, OP_BAR_OUTLINE = 5, OP_BAR_FILL = 6, OP_URGENT_BORDER = 7, OP_REISSUE_BARS = 8, OP_CROSS = 9, OP_PROGRESS_META = 10, OP_SCHEDULE_PROGRESS_META = 11, OP_DOTTED_RECT = 12, OP_URGENT_TAB = 13, OP_DOTTED_LINE = 14 };
enum FontType : uint8_t { FONT_MONO = 0, FONT_BOLD = 1, FONT_SMALL = 2 };
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

// Lightweight diagnostics: kept simple to avoid changing the proven HTTP path.
String lastFaultStage = "none";
String lastFaultDetail = "";
int lastHttpCode = 0;
String lastHttpUrl = "";
unsigned long lastHttpMillis = 0;
unsigned long lastSuccessfulMetaMillis = 0;
unsigned long lastSuccessfulJobMillis = 0;
unsigned long lastSuccessfulAckMillis = 0;
unsigned long lastRenderMillis = 0;
int opsDropped = 0;
unsigned long lastWifiRetry = 0;
const unsigned long wifiRetryInterval = 60000UL;

struct ProgressRegion {
  uint8_t kind; // 0 task epoch, 1 schedule minutes
  int16_t x, y, w, h;
  uint32_t a;
  uint32_t b;
};

const int MAX_PROGRESS_REGIONS = 16;
ProgressRegion progressRegions[MAX_PROGRESS_REGIONS];
int progressRegionCount = 0;
unsigned long lastTimedMainRefresh = 0;
const unsigned long timedMainRefreshInterval = 300000UL; // 5 min

// Display/network isolation. Device B favours slow-safe behaviour over responsiveness.
bool displayBusyPhase = false;
unsigned long displayQuietUntil = 0;
const unsigned long displaySettleMs = 10000UL;
const unsigned long displayPowerSettleMs = 250UL;
const unsigned long displayBusyTimeoutMs = 60000UL;


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
bool updateDisplayFromRenderOps();
void renderCurrentOpsPaged();
uint16_t mapColor(uint8_t colorCode);
float progressFraction(const ProgressRegion &r);
void drawDynamicProgressFillsOnce();
void drawDottedLine(int x1, int y1, int x2, int y2, uint16_t color, int dash = 4, int gap = 4);
void drawDottedRect(const RenderOp &ro);
void drawUrgentTab(const RenderOp &ro);
void reconnectPreferredIfNeeded(bool force = false);
void setFault(const String &stage, const String &detail);

void setFault(const String &stage, const String &detail) {
  lastFaultStage = stage;
  lastFaultDetail = detail;
  Serial.print("FAULT "); Serial.print(stage); Serial.print(": "); Serial.println(detail);
}

bool safeForNetwork() {
  if (refreshInProgress) return false;
  if (displayBusyPhase) return false;
  if (millis() < displayQuietUntil) return false;
  if (usingFallbackAP) return false;
  if (WiFi.status() != WL_CONNECTED) return false;
  return true;
}

bool waitForDisplay() {
  unsigned long start = millis();

  while (digitalRead(BUSY) == HIGH) {
    delay(10);

    if (millis() - start > displayBusyTimeoutMs) {
      Serial.println("Busy Timeout!");
      setFault("display", "BUSY_TIMEOUT");
      displayQuietUntil = millis() + displaySettleMs;
      return false;
    }
  }

  return true;
}

void displayWake() {
  pinMode(PWR_PIN, OUTPUT);
  digitalWrite(PWR_PIN, HIGH);
  delay(displayPowerSettleMs);
}

void displaySleep() {
  display.hibernate();
  delay(500);
  digitalWrite(PWR_PIN, LOW);
  displayQuietUntil = millis() + displaySettleMs;
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

void printMemoryDiagnostics(const char* label) {
  Serial.print("=== MEMORY DIAG: "); Serial.print(label); Serial.println(" ===");
  Serial.print("psramFound: "); Serial.println(psramFound() ? "YES" : "NO");
  Serial.print("PSRAM size: "); Serial.println(ESP.getPsramSize());
  Serial.print("Free PSRAM: "); Serial.println(ESP.getFreePsram());
  Serial.print("Free heap: "); Serial.println(ESP.getFreeHeap());
  Serial.print("Largest free internal block: "); Serial.println(heap_caps_get_largest_free_block(MALLOC_CAP_INTERNAL));
  Serial.print("Largest free 8-bit block: "); Serial.println(heap_caps_get_largest_free_block(MALLOC_CAP_8BIT));
  Serial.print("Largest free SPIRAM block: "); Serial.println(heap_caps_get_largest_free_block(MALLOC_CAP_SPIRAM));
  Serial.println("=== MEMORY DIAG END ===");
}

bool configureSecureClient(WiFiClientSecure &client) {
  client.setInsecure();
  client.setHandshakeTimeout(30);
  Serial.println("TLS mode: INSECURE");
  return true;
}

void printTlsContext() {
  printMemoryDiagnostics("before TLS");
  time_t nowT; time(&nowT);
  Serial.print("Unix time: "); Serial.println((long)nowT);
  Serial.print("Time synced flag: "); Serial.println(timeSynced ? "YES" : "NO");
}

bool runHttpsPreflight(const String &host) {
  Serial.println("=== HTTPS PREFLIGHT START ===");
  Serial.print("WiFi.status: "); Serial.println(WiFi.status());
  Serial.print("SSID: "); Serial.println(WiFi.SSID());
  Serial.print("Local IP: "); Serial.println(WiFi.localIP());
  Serial.print("Gateway: "); Serial.println(WiFi.gatewayIP());
  Serial.print("DNS1: "); Serial.println(WiFi.dnsIP(0));
  Serial.print("DNS2: "); Serial.println(WiFi.dnsIP(1));
  Serial.print("RSSI: "); Serial.println(WiFi.RSSI());

  IPAddress resolvedIP;
  Serial.print("DNS lookup: "); Serial.println(host);
  bool dnsOk = WiFi.hostByName(host.c_str(), resolvedIP);
  Serial.print("DNS OK: "); Serial.println(dnsOk ? "YES" : "NO");
  if (!dnsOk) {
    setFault("dns", "hostByName_failed");
    Serial.println("=== HTTPS PREFLIGHT END ===");
    return false;
  }
  Serial.print("Resolved IP: "); Serial.println(resolvedIP);

  WiFiClient tcp;
  Serial.print("TCP connect to "); Serial.print(host); Serial.println(":443");
  bool tcpOk = tcp.connect(host.c_str(), 443);
  Serial.print("TCP443 OK: "); Serial.println(tcpOk ? "YES" : "NO");
  tcp.stop();
  if (!tcpOk) {
    setFault("tcp", "connect_443_failed");
    Serial.println("=== HTTPS PREFLIGHT END ===");
    return false;
  }
  Serial.println("=== HTTPS PREFLIGHT END ===");
  return true;
}

bool httpsGETAttempt(String url, String &out) {
  WiFiClientSecure client;
  configureSecureClient(client);

  HTTPClient http;
  http.setTimeout(20000);
  http.setReuse(false);

  Serial.println("HTTP begin call...");
  if (!http.begin(client, url)) {
    setFault("connection", "http_begin_failed_insecure");
    Serial.println("HTTP begin failed");
    return false;
  }

  Serial.println("HTTP GET call...");
  int code = http.GET();
  Serial.println("HTTP GET returned");
  lastHttpCode = code;
  Serial.print("HTTP CODE: "); Serial.println(code);
  if (code < 0) {
    Serial.print("HTTP error text: "); Serial.println(http.errorToString(code));
  }

  if (code != 200) {
    setFault("connection", String("https_insecure_") + String(code));
    http.end();
    return false;
  }

  out = http.getString();
  Serial.print("Payload length: "); Serial.println(out.length());
  http.end();
  lastFaultStage = "none";
  lastFaultDetail = "";
  return true;
}

bool httpGET(String url, String &out) {
  lastHttpUrl = url;
  lastHttpMillis = millis();
  lastHttpCode = 0;
  Serial.print("HTTPS GET: "); Serial.println(url);
  if (!safeForNetwork()) {
    if (usingFallbackAP) setFault("connection", "fallback_ap");
    else if (WiFi.status() != WL_CONNECTED) setFault("connection", "wifi_not_connected");
    else setFault("connection", "display_quiet");
    return false;
  }

  printTlsContext();
  if (!runHttpsPreflight("device-b-relay.onrender.com")) return false;

  Serial.println("HTTPS attempt: insecure");
  return httpsGETAttempt(url, out);
}

bool httpsPOSTAttempt(String url) {
  WiFiClientSecure client;
  configureSecureClient(client);
  HTTPClient http;
  http.setTimeout(20000);
  http.setReuse(false);
  if (!http.begin(client, url)) {
    setFault("connection", "http_post_begin_failed_insecure");
    return false;
  }
  int code = http.POST("");
  lastHttpCode = code;
  Serial.print("HTTP POST CODE: "); Serial.println(code);
  if (code < 0) {
    Serial.print("HTTP error text: "); Serial.println(http.errorToString(code));
  }
  http.end();
  if (!(code >= 200 && code < 300)) {
    setFault("connection", String("https_post_insecure_") + String(code));
    return false;
  }
  return true;
}

bool httpPOSTempty(String url) {
  lastHttpUrl = url;
  lastHttpMillis = millis();
  lastHttpCode = 0;
  Serial.print("HTTPS POST: "); Serial.println(url);
  if (!safeForNetwork()) {
    if (usingFallbackAP) setFault("connection", "fallback_ap");
    else if (WiFi.status() != WL_CONNECTED) setFault("connection", "wifi_not_connected");
    else setFault("connection", "display_quiet");
    return false;
  }

  printTlsContext();
  if (!runHttpsPreflight("device-b-relay.onrender.com")) return false;

  Serial.println("HTTPS POST attempt: insecure");
  return httpsPOSTAttempt(url);
}

bool fetchRelayMetaNow(RelayMeta &meta) {
  String payload;
  String url = String(relayBaseUrl) + "/api/meta?token=" + relayToken;
  if (!httpGET(url, payload)) return false;
  DynamicJsonDocument doc(2048);
  DeserializationError err = deserializeJson(doc, payload);
  if (err) { setFault("json", String("meta_") + err.c_str()); return false; }
  meta.pageRevision = doc["page_revision"] | 0;
  meta.pageType = doc["page_type"] | "main";
  meta.jobId = doc["job_id"] | 0;
  meta.refreshRequested = doc["refresh_requested"] | 0;
  meta.forceOTA = doc["force_ota"] | 0;
  meta.firmwareVersionFromRelay = doc["firmware_version"] | "";
  lastSuccessfulMetaMillis = millis();
  return true;
}

bool fetchRenderJobNow(unsigned long jobId) {
  String payload;
  String url = String(relayBaseUrl) + "/api/render_job?token=" + relayToken + "&job_id=" + String(jobId);
  if (!httpGET(url, payload)) return false;
  DynamicJsonDocument doc(65536);
  DeserializationError err = deserializeJson(doc, payload);
  if (err) { setFault("json", String("job_") + err.c_str()); return false; }
  if (!(doc["ok"] | false)) { setFault("json", "job_not_ok"); return false; }
  currentPageType = doc["page_type"].as<String>();
  JsonArray ops = doc["payload"]["ops"].as<JsonArray>();
  renderOpCount = 0;
  progressRegionCount = 0;
  opsDropped = 0;
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

    if (renderOpCount >= MAX_OPS) { opsDropped++; setFault("memory", "ops_overflow"); break; }
    RenderOp &ro = renderOps[renderOpCount];
    ro.x = op["x"] | 0; ro.y = op["y"] | 0; ro.x2 = op["x1"] | op["x2"] | 0; ro.y2 = op["y1"] | op["y2"] | 0;
    ro.w = op["w"] | 0; ro.h = op["h"] | 0; ro.value = op["count"] | 0;
    String color = op["color"] | "black"; ro.color = (color == "red") ? COLOR_RED : ((color == "white") ? COLOR_WHITE : COLOR_BLACK);
    String font = op["font"] | "mono";
    if (font == "bold") ro.font = FONT_BOLD;
    else if (font == "small") ro.font = FONT_SMALL;
    else ro.font = FONT_MONO;
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
    else { opsDropped++; continue; }
    renderOpCount++;
  }
  lastSuccessfulJobMillis = millis();
  return true;
}

bool ackCurrentJob(unsigned long jobId) {
  String url = String(relayBaseUrl) + "/api/ack_job?token=" + relayToken + "&job_id=" + String(jobId);
  bool ok = httpPOSTempty(url);
  if (ok) lastSuccessfulAckMillis = millis();
  return ok;
}

uint16_t mapColor(uint8_t colorCode) {
  if (colorCode == COLOR_RED) return GxEPD_RED;
  if (colorCode == COLOR_WHITE) return GxEPD_WHITE;
  return GxEPD_BLACK;
}

void applyFont(uint8_t fontCode) {
  display.setTextSize(1);
  if (fontCode == FONT_BOLD) {
    display.setFont(&FreeMonoBold9pt7b);
  } else if (fontCode == FONT_SMALL) {
    // Built-in 5x7 Adafruit_GFX font. This uses no extra font file and is
    // substantially smaller than FreeMono9pt, so it is suitable for calendar cells.
    display.setFont(NULL);
  } else {
    display.setFont(&FreeMono9pt7b);
  }
}


void drawDottedLine(int x1, int y1, int x2, int y2, uint16_t color, int dash, int gap) {
  if (x1 == x2) {
    int yStart = min(y1, y2);
    int yEnd = max(y1, y2);
    for (int y = yStart; y <= yEnd; y += dash + gap) {
      display.drawLine(x1, y, x2, min(y + dash, yEnd), color);
    }
  } else if (y1 == y2) {
    int xStart = min(x1, x2);
    int xEnd = max(x1, x2);
    for (int x = xStart; x <= xEnd; x += dash + gap) {
      display.drawLine(x, y1, min(x + dash, xEnd), y2, color);
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
  int s = ro.value > 0 ? ro.value : min(ro.w, ro.h);
  if (s <= 0) s = 10;
  uint16_t c = mapColor(ro.color);
  // Filled right-angle triangle in top-right corner. Draw scan lines for broad GxEPD2 compatibility.
  for (int i = 0; i < s; i++) {
    display.drawLine(ro.x + ro.w - i, ro.y, ro.x + ro.w, ro.y + i, c);
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
        drawDottedRect(ro);
        break;
      case OP_URGENT_TAB:
        drawUrgentTab(ro);
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
  do {
    executeRenderOpsOnce();
    drawDynamicProgressFillsOnce();
  } while (display.nextPage());
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

void drawDynamicProgressFillsOnce() {
  if (currentPageType != "main") return;
  if (progressRegionCount <= 0) return;
  if (!timeSynced) return;

  for (int i = 0; i < progressRegionCount; i++) {
    const ProgressRegion &pr = progressRegions[i];
    float frac = progressFraction(pr);
    if (frac < 0.0f) continue;
    int fill = (int)(frac * pr.w);
    if (fill < 0) fill = 0;
    if (fill > pr.w) fill = pr.w;
    if (fill > 0) {
      display.fillRect(pr.x, pr.y, fill, pr.h, GxEPD_BLACK);
    }
  }
}

bool updateDisplayFromRenderOps() {
  if (refreshInProgress || displayBusyPhase) {
    setFault("render", "refresh_already_active");
    return false;
  }

  refreshInProgress = true;
  displayBusyPhase = true;

  Serial.println("DISPLAY RENDER START");
  Serial.print("OPS TO RENDER: ");
  Serial.println(renderOpCount);

  displayWake();
  Serial.println("DISPLAY INIT");
  display.init();
  delay(300);

  // Do not perform an external BUSY wait before firstPage().
  // GxEPD2 handles BUSY internally during firstPage()/nextPage().
  // The explicit pre-render BUSY check caused false BUSY_TIMEOUT aborts
  // before any drawing reached the panel.
  Serial.println("DISPLAY FIRSTPAGE START");
  renderCurrentOpsPaged();
  Serial.println("DISPLAY RENDER COMPLETE");

  displaySleep();

  lastTimedMainRefresh = millis();
  lastRenderMillis = millis();

  displayBusyPhase = false;
  refreshInProgress = false;
  return true;
}

void updateBootStatusScreen(const String &line1, const String &line2) {
  // Clean scheduler rule: routine faults are NOT drawn to the e-paper.
  // Use serial/local web diagnostics instead. This prevents display faults
  // from cascading into network/TLS failures during boot or retry loops.
  Serial.print("STATUS: ");
  Serial.print(line1);
  if (line2.length() > 0) {
    Serial.print(" | ");
    Serial.print(line2);
  }
  Serial.println();
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
  page += "<br>Display busy phase: "; page += displayBusyPhase ? "yes" : "no";
  page += "<br>Display quiet ms left: ";
  page += String(millis() < displayQuietUntil ? (displayQuietUntil - millis()) : 0);
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
  page += "</p>";
  page += "<form action='/refresh'><input type='submit' value='Refresh Device'></form>";
  page += "<form action='/fetch_now'><input type='submit' value='Fetch Relay Now'></form>";
  page += "<form action='/retry_wifi'><input type='submit' value='Retry WiFi'></form>";
  page += "</body></html>";
  return page;
}

void handleRoot() { server.send(200, "text/html", localWebpage()); }
void handleRefresh() {
  if (refreshInProgress || displayBusyPhase || millis() < displayQuietUntil) {
    renderJobQueued = true;
  } else {
    if (targetJobId == 0) deviceState = STATE_POLL_META;
    else renderJobQueued = true;
  }
  server.sendHeader("Location", "/"); server.send(303);
}
void handleFetchNow() {
  if (!refreshInProgress && !displayBusyPhase && millis() >= displayQuietUntil) {
    deviceState = STATE_POLL_META;
  }
  server.sendHeader("Location", "/");
  server.send(303);
}
void handleRetryWiFi() { reconnectPreferredIfNeeded(true); server.sendHeader("Location", "/"); server.send(303); }


void reconnectPreferredIfNeeded(bool force) {
  if (!force && (millis() - lastWifiRetry < wifiRetryInterval)) return;
  lastWifiRetry = millis();
  if (!force && !usingFallbackAP && WiFi.status() == WL_CONNECTED) return;

  Serial.println("WIFI RETRY START");
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
      Serial.print("WIFI RETRY OK: "); Serial.println(activeAddress);
      syncTimeNow();
      return;
    }
  }
  setFault("connection", "wifi_retry_failed");
  startFallbackAP();
}

void handleButtonRefresh() {
  static unsigned long lastButtonAction = 0;
  if (digitalRead(REFRESH_BUTTON) == LOW) {
    if (millis() - lastButtonAction > 1200) {
      renderJobQueued = true;
      lastButtonAction = millis();
    }
  }
}

void runStateMachine() {
  switch (deviceState) {
    case STATE_IDLE:
      // Network manager: poll only when display is fully idle and post-display quiet period has expired.
      if (safeForNetwork() && (millis() - lastMetaPoll > metaPollInterval)) {
        deviceState = STATE_POLL_META;
      }
      else if (safeForNetwork() && renderJobQueued && targetJobId > 0) {
        deviceState = STATE_FETCH_JOB;
      }
      break;

    case STATE_POLL_META:
      // Network phase only. No display calls here.
      if (!safeForNetwork()) {
        deviceState = STATE_IDLE;
        break;
      }
      if (fetchRelayMetaNow(latestMeta)) {
        Serial.println("POLL META OK");
        if (latestMeta.jobId > lastAckedJobId || latestMeta.refreshRequested) {
          targetJobId = latestMeta.jobId;
          renderJobQueued = true;
        }
        // OTA intentionally removed. force_ota from relay is ignored.
      } else {
        Serial.println("POLL META FAIL");
        if (lastFaultStage == "none") setFault("connection", "meta_fetch_failed");
      }
      lastMetaPoll = millis();
      deviceState = STATE_IDLE;
      break;

    case STATE_FETCH_JOB:
      // Network phase only. A successful fetch populates the local render cache.
      if (!safeForNetwork()) {
        deviceState = STATE_IDLE;
        break;
      }
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
      // Display phase only. No HTTP/JSON/WiFi calls inside updateDisplayFromRenderOps().
      if (!refreshInProgress && !displayBusyPhase && renderOpCount > 0) {
        if (updateDisplayFromRenderOps()) {
          deviceState = STATE_ACK_JOB;
        } else {
          // Do not ACK a job that did not render.
          deviceState = STATE_IDLE;
        }
      } else {
        setFault("render", "no_ops_or_busy");
        deviceState = STATE_IDLE;
      }
      break;

    case STATE_ACK_JOB:
      // Network phase after display quiet period. Do not fail ACK just because
      // the e-paper has only just been powered down.
      if (!safeForNetwork()) {
        // Stay in ACK state until safe, but keep serving the local web page.
        break;
      }
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
  Serial.println("BOOT: DEVICE_B_CLEAN_SCHEDULER_V3_HTTPS_PSRAM_DIAG_NO_OTA");
  printMemoryDiagnostics("boot");

  pinMode(PWR_PIN, OUTPUT);
  digitalWrite(PWR_PIN, LOW);
  pinMode(REFRESH_BUTTON, INPUT_PULLUP);

  // Local server is started regardless of relay success so diagnostics are available.
  connectPreferredOrFallback();
  Serial.print("WIFI MODE: ");
  Serial.println(usingFallbackAP ? "Fallback AP" : "Preferred WiFi");
  Serial.print("ADDRESS: ");
  Serial.println(activeAddress);

  server.on("/", handleRoot);
  server.on("/refresh", handleRefresh);
  server.on("/fetch_now", handleFetchNow);
  server.on("/retry_wifi", handleRetryWiFi);
  server.begin();
  Serial.println("WEB SERVER STARTED");

  if (!usingFallbackAP) {
    syncTimeNow();
  }

  // Clean scheduler rule: do not draw boot/fault screens before networking.
  // Fetch first; render only after a complete job has been cached.
  if (safeForNetwork() && fetchRelayMetaNow(latestMeta)) {
    Serial.println("META OK");
    targetJobId = latestMeta.jobId;
    if (targetJobId > 0 && fetchRenderJobNow(targetJobId)) {
      Serial.print("JOB FETCH OK: ");
      Serial.println(targetJobId);
      deviceState = STATE_RENDER_JOB;
      renderJobQueued = true;
    } else {
      Serial.println("BOOT JOB FETCH FAIL OR NO JOB");
      deviceState = STATE_IDLE;
    }
  } else {
    Serial.println("BOOT META FETCH SKIPPED/FAILED");
    // Leave the previous e-paper image intact. Diagnostics are available via serial/local web.
    deviceState = STATE_IDLE;
  }

  lastMetaPoll = millis();
  lastTimedMainRefresh = millis();
}

void loop() {
  server.handleClient();

  if (!usingFallbackAP && (millis() - lastTimeSync > timeSyncInterval)) {
    syncTimeNow();
  }

  if (!refreshInProgress && !displayBusyPhase && millis() >= displayQuietUntil) {
    reconnectPreferredIfNeeded(false);
  }
  handleButtonRefresh();
  runStateMachine();

  if (currentPageType == "main" &&
      !refreshInProgress &&
      !displayBusyPhase &&
      millis() >= displayQuietUntil &&
      !renderJobQueued &&
      (millis() - lastTimedMainRefresh > timedMainRefreshInterval)) {
    Serial.println("TIMED MAIN FULL REFRESH");
    if (renderOpCount > 0) updateDisplayFromRenderOps();
  }

  delay(1);
}
