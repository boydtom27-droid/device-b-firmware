/*
  Pocket Device firmware - PD page-cache build

  Hardware target:
    - ESP32-S3 Waveshare ESP32-S3-DEV-KIT-N8R8/N8RX style board
    - Waveshare Pico-ePaper-4.2, 400x300, B/W with 4 grey-scale source payload

  Wiring used by this build:
    Display BUSY -> GPIO7
    Display RST  -> GPIO8
    Display DC   -> GPIO9
    Display CS   -> GPIO10
    Display DIN  -> GPIO11
    Display SCK  -> GPIO12
    Display GND  -> GND
    Display VCC  -> 3V3
    Button       -> GPIO14 to GND, INPUT_PULLUP

  Relay endpoints used by this firmware:
    /api/pd/meta
    /api/pd/page?page=task|idea|calendar|scribble
    /api/pd/ack

  Notes:
    - The relay sends PGB1 binary pages: 400x300, 2 bits per pixel.
    - This firmware stores the 2bpp greyscale pages in PSRAM and renders them
      to the black/white panel using ordered spatial dithering. This preserves
      the relay's greyscale intent even if the installed GxEPD2 driver does not
      expose native 4-grey waveform drawing for this exact panel revision.
    - If your GxEPD2 library does not contain GxEPD2_420_GDEY042T81, change the
      EPD class below to GxEPD2_420 or GxEPD2_420_M01 and retest orientation.
*/

#include <WiFi.h>
#include <WebServer.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>
#include <ArduinoJson.h>
#include <SPI.h>
#include <time.h>
#include <esp_heap_caps.h>
#include <GxEPD2_BW.h>

#define BUILD_VERSION "PD_PAGE_CACHE_V3_EVENT_QUEUE_TIMEFIX"

#define EPD_BUSY 7
#define EPD_RST  8
#define EPD_DC   9
#define EPD_CS   10
#define EPD_MOSI 11
#define EPD_SCK  12

#define BTN_PAGE 14

// Haptic module logic-input pins. Connect module IN pins here; connect module
// VCC to a suitable motor supply and module GND to ESP32 GND. If using only one
// module, use HAPTIC_X and leave the other pins unconnected.
#define HAPTIC_X 4
#define HAPTIC_Y 2
#define HAPTIC_Z 21
#define HAPTIC_ACTIVE_HIGH true

#define DISPLAY_ROTATION 0
#define PD_W 400
#define PD_H 300
#define PGB_HEADER_BYTES 10
#define PGB_DATA_BYTES (((PD_W * PD_H * 2) + 7) / 8)
#define PGB_TOTAL_BYTES (PGB_HEADER_BYTES + PGB_DATA_BYTES)

// Preferred display class for Waveshare 4.2" V2 style 400x300 black/white panel.
// Alternatives if compilation fails: GxEPD2_420, GxEPD2_420_M01.
GxEPD2_BW<GxEPD2_420_GDEY042T81, GxEPD2_420_GDEY042T81::HEIGHT> display(
  GxEPD2_420_GDEY042T81(EPD_CS, EPD_DC, EPD_RST, EPD_BUSY)
);

WebServer server(80);

const char* relayBaseUrl = "https://device-b-relay.onrender.com";
const char* relayToken = "abc123xyz789";

struct SavedNetwork {
  const char* ssid;
  const char* password;
};
SavedNetwork preferredNetworks[] = {
  {"Tomspot", "Tom00001"},
  {"VM6269662", "FollyDaRabbit123"},
  {"guest-dog", "givemeinternet"},
};
const int preferredNetworkCount = sizeof(preferredNetworks) / sizeof(preferredNetworks[0]);

const char* pageKeys[] = {"task", "idea", "calendar", "calendar_week", "scribble", "alarm"};
const char* pageTitles[] = {"Tasks", "Ideas", "Calendar", "Week", "Scribble", "Alarm"};
const int pageCount = 6;
const int normalPageCount = 5;   // alarm is a hidden/popup page
const int alarmPageIndex = 5;

struct CachedPage {
  const char* key;
  uint32_t revision;
  bool loaded;
  uint8_t* payload;   // PGB1 data body only, not header
};
CachedPage pages[6];

int currentPage = 0;
int lastNormalPage = 0;
uint32_t bundleRevision = 0;
uint32_t lastAckedBundleRevision = 0;
unsigned long lastPollMs = 0;
const unsigned long pollIntervalMs = 60000UL;
unsigned long lastTimeSyncMs = 0;
const unsigned long timeSyncIntervalMs = 21600000UL; // 6 h
bool timeSynced = false;

const int MAX_PATTERN_STEPS = 16;
const int MAX_EVENTS = 20;
const int MAX_FIRED_KEYS = 32;

struct PdEvent {
  bool active;
  bool popUp;
  time_t epoch;
  char key[96];
  char type[16];
  int pattern[MAX_PATTERN_STEPS];
  int patternCount;
};

PdEvent events[MAX_EVENTS];
int eventCount = 0;
uint32_t alertRevision = 0;
uint32_t lastAckedAlertRevision = 0;
char firedKeys[MAX_FIRED_KEYS][96];
int firedKeyPos = 0;
String activeAlarmKey = "";

bool usingFallbackAP = false;
String activeNetworkName = "";
String activeAddress = "";
String lastFaultStage = "none";
String lastFaultDetail = "";
int lastHttpCode = 0;
String lastHttpUrl = "";
unsigned long lastRenderMs = 0;
unsigned long lastMetaOkMs = 0;
unsigned long lastPageOkMs = 0;
unsigned long lastButtonChangeMs = 0;
bool displayBusy = false;

void setFault(const String& stage, const String& detail) {
  lastFaultStage = stage;
  lastFaultDetail = detail;
  Serial.print("FAULT "); Serial.print(stage); Serial.print(": "); Serial.println(detail);
}

void allocatePageBuffers() {
  for (int i = 0; i < pageCount; i++) {
    pages[i].key = pageKeys[i];
    pages[i].revision = 0;
    pages[i].loaded = false;
    pages[i].payload = (uint8_t*)heap_caps_malloc(PGB_DATA_BYTES, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!pages[i].payload) pages[i].payload = (uint8_t*)malloc(PGB_DATA_BYTES);
    if (!pages[i].payload) {
      setFault("memory", String("page_alloc_") + pageKeys[i]);
    } else {
      memset(pages[i].payload, 0xFF, PGB_DATA_BYTES);
    }
  }
}

bool tryConnectOneNetwork(const char* ssid, const char* password, unsigned long timeoutMs) {
  if (!ssid || strlen(ssid) == 0) return false;
  Serial.print("WiFi trying: "); Serial.println(ssid);
  WiFi.mode(WIFI_STA);
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

void startFallbackAP() {
  WiFi.mode(WIFI_AP);
  WiFi.softAP("PocketDevice", "tasks123");
  usingFallbackAP = true;
  activeNetworkName = "Fallback AP";
  activeAddress = WiFi.softAPIP().toString();
}

void connectPreferredOrFallback() {
  usingFallbackAP = false;
  WiFi.mode(WIFI_STA);
  WiFi.disconnect(true, true);
  delay(300);
  for (int i = 0; i < preferredNetworkCount; i++) {
    if (tryConnectOneNetwork(preferredNetworks[i].ssid, preferredNetworks[i].password, 8000)) {
      activeNetworkName = preferredNetworks[i].ssid;
      activeAddress = WiFi.localIP().toString();
      Serial.print("WiFi OK: "); Serial.println(activeAddress);
      return;
    }
  }
  setFault("wifi", "preferred_failed_fallback_ap");
  startFallbackAP();
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
      Serial.println("Time sync OK");
      timeSynced = true;
      lastTimeSyncMs = millis();
      return;
    }
    delay(250);
  }
  timeSynced = false;
  setFault("time", "ntp_failed");
}

bool safeForNetwork() {
  if (displayBusy) return false;
  if (usingFallbackAP) return false;
  if (WiFi.status() != WL_CONNECTED) return false;
  return true;
}

bool configureSecureClient(WiFiClientSecure& client) {
  client.setInsecure();
  client.setHandshakeTimeout(30);
  return true;
}

bool httpGETText(const String& url, String& out) {
  out = "";
  lastHttpUrl = url;
  lastHttpCode = 0;
  if (!safeForNetwork()) {
    setFault("connection", usingFallbackAP ? "fallback_ap" : "wifi_or_display_busy");
    return false;
  }
  WiFiClientSecure client;
  configureSecureClient(client);
  HTTPClient http;
  http.setTimeout(25000);
  http.setReuse(false);
  if (!http.begin(client, url)) {
    setFault("http", "begin_failed");
    return false;
  }
  int code = http.GET();
  lastHttpCode = code;
  if (code != 200) {
    setFault("http", String("GET_") + code);
    http.end();
    return false;
  }
  out = http.getString();
  http.end();
  lastFaultStage = "none";
  lastFaultDetail = "";
  return true;
}

bool httpGETBinaryToBuffer(const String& url, uint8_t* outBuf, size_t expectedLen, size_t& got) {
  got = 0;
  lastHttpUrl = url;
  lastHttpCode = 0;
  if (!safeForNetwork()) {
    setFault("connection", usingFallbackAP ? "fallback_ap" : "wifi_or_display_busy");
    return false;
  }
  WiFiClientSecure client;
  configureSecureClient(client);
  HTTPClient http;
  http.setTimeout(35000);
  http.setReuse(false);
  if (!http.begin(client, url)) {
    setFault("http", "binary_begin_failed");
    return false;
  }
  int code = http.GET();
  lastHttpCode = code;
  if (code != 200) {
    setFault("http", String("BIN_GET_") + code);
    http.end();
    return false;
  }
  int len = http.getSize();
  if (len > 0 && (size_t)len != expectedLen) {
    setFault("http", String("binary_size_") + len);
    http.end();
    return false;
  }
  WiFiClient* stream = http.getStreamPtr();
  unsigned long lastProgress = millis();
  while (got < expectedLen) {
    int avail = stream->available();
    if (avail > 0) {
      size_t toRead = expectedLen - got;
      if (toRead > (size_t)avail) toRead = (size_t)avail;
      if (toRead > 1024) toRead = 1024;
      size_t n = stream->readBytes(outBuf + got, toRead);
      if (n > 0) {
        got += n;
        lastProgress = millis();
      }
    } else {
      delay(5);
    }
    if (millis() - lastProgress > 15000UL) {
      setFault("http", "binary_timeout");
      http.end();
      return false;
    }
  }
  http.end();
  lastFaultStage = "none";
  lastFaultDetail = "";
  return true;
}

void hapticSet(bool on) {
  int level = (on == HAPTIC_ACTIVE_HIGH) ? HIGH : LOW;
  digitalWrite(HAPTIC_X, level);
  digitalWrite(HAPTIC_Y, level);
  digitalWrite(HAPTIC_Z, level);
}

void hapticOff() {
  hapticSet(false);
}

bool keyWasFired(const char* key) {
  if (!key || key[0] == '\0') return true;
  for (int i = 0; i < MAX_FIRED_KEYS; i++) {
    if (strcmp(firedKeys[i], key) == 0) return true;
  }
  return false;
}

void markKeyFired(const char* key) {
  if (!key || key[0] == '\0' || keyWasFired(key)) return;
  strlcpy(firedKeys[firedKeyPos], key, sizeof(firedKeys[firedKeyPos]));
  firedKeyPos = (firedKeyPos + 1) % MAX_FIRED_KEYS;
}

void runHapticPattern(const int* pattern, int patternCount, const char* label) {
  if (!pattern || patternCount <= 0) return;
  Serial.print("HAPTIC pattern for "); Serial.println(label ? label : "event");
  for (int i = 0; i < patternCount; i++) {
    bool on = (i % 2) == 0;
    hapticSet(on);
    unsigned long dur = (unsigned long)pattern[i];
    if (dur > 3000UL) dur = 3000UL;
    unsigned long start = millis();
    while (millis() - start < dur) {
      server.handleClient();
      delay(5);
    }
  }
  hapticOff();
}

void clearEvents() {
  eventCount = 0;
  for (int i = 0; i < MAX_EVENTS; i++) {
    events[i].active = false;
    events[i].popUp = false;
    events[i].epoch = 0;
    events[i].key[0] = '\0';
    events[i].type[0] = '\0';
    events[i].patternCount = 0;
  }
}

void parseEventsFromMeta(JsonArray arr) {
  clearEvents();
  if (arr.isNull()) return;
  int n = 0;
  for (JsonObject ev : arr) {
    if (n >= MAX_EVENTS) break;
    const char* key = ev["key"] | "";
    const char* type = ev["type"] | "prompt";
    if ((!type || type[0] == '\0') && !ev["state"].isNull()) type = ev["state"];
    if (!key || key[0] == '\0') continue;
    events[n].active = true;
    events[n].popUp = ev["pop_up"] | false;
    events[n].epoch = (time_t)(ev["epoch"] | 0);
    strlcpy(events[n].key, key, sizeof(events[n].key));
    strlcpy(events[n].type, type, sizeof(events[n].type));
    events[n].patternCount = 0;
    JsonArray pat = ev["pattern_ms"].as<JsonArray>();
    for (JsonVariant v : pat) {
      if (events[n].patternCount >= MAX_PATTERN_STEPS) break;
      int ms = v.as<int>();
      if (ms < 20) ms = 20;
      if (ms > 3000) ms = 3000;
      events[n].pattern[events[n].patternCount++] = ms;
    }
    n++;
  }
  eventCount = n;
  Serial.print("Events loaded: "); Serial.println(eventCount);
}

void acknowledgeActiveAlarm() {
  if (currentPage == alarmPageIndex && activeAlarmKey.length() > 0) {
    markKeyFired(activeAlarmKey.c_str());
    Serial.print("ALARM DISMISS "); Serial.println(activeAlarmKey);
  }
  activeAlarmKey = "";
  hapticOff();
}

void processEvents(bool allowDraw) {
  if (!timeSynced) return;
  time_t nowT; time(&nowT);
  for (int i = 0; i < eventCount; i++) {
    PdEvent &ev = events[i];
    if (!ev.active || ev.epoch <= 0) continue;
    if (nowT < ev.epoch) continue;
    if (keyWasFired(ev.key)) continue;

    bool isAlert = strcmp(ev.type, "alert") == 0;
    if (ev.popUp && isAlert && allowDraw && pages[alarmPageIndex].loaded) {
      if (currentPage != alarmPageIndex && currentPage < normalPageCount) lastNormalPage = currentPage;
      currentPage = alarmPageIndex;
      activeAlarmKey = String(ev.key);
      drawCurrentPageToDisplay(true);
    }

    runHapticPattern(ev.pattern, ev.patternCount, ev.key);
    // Alerts are not self-repeating. Nag repetition is relay-side via a new key
    // in the next overdue interval. Prompt/timekeeping buzzes are also one-shot.
    markKeyFired(ev.key);
    break;
  }
}

bool httpPOSTAck(uint32_t rev) {
  if (!safeForNetwork()) return false;
  String url = String(relayBaseUrl) + "/api/pd/ack?token=" + relayToken + "&bundle_revision=" + String(rev) + "&alert_revision=" + String(alertRevision) + "&page=" + pageKeys[currentPage];
  WiFiClientSecure client;
  configureSecureClient(client);
  HTTPClient http;
  http.setTimeout(15000);
  if (!http.begin(client, url)) return false;
  int code = http.POST("");
  http.end();
  if (code >= 200 && code < 300) {
    lastAckedAlertRevision = alertRevision;
    return true;
  }
  return false;
}

int pageIndexByKey(const char* key) {
  for (int i = 0; i < pageCount; i++) {
    if (strcmp(key, pageKeys[i]) == 0) return i;
  }
  return -1;
}

bool fetchOnePage(const char* key, uint32_t revision, bool force) {
  int idx = pageIndexByKey(key);
  if (idx < 0 || !pages[idx].payload) return false;
  if (!force && pages[idx].loaded && pages[idx].revision == revision) return true;

  uint8_t* tmp = (uint8_t*)heap_caps_malloc(PGB_TOTAL_BYTES, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
  if (!tmp) tmp = (uint8_t*)malloc(PGB_TOTAL_BYTES);
  if (!tmp) {
    setFault("memory", "tmp_pgb_alloc");
    return false;
  }

  String url = String(relayBaseUrl) + "/api/pd/page?token=" + relayToken + "&page=" + key;
  size_t got = 0;
  bool ok = httpGETBinaryToBuffer(url, tmp, PGB_TOTAL_BYTES, got);
  if (!ok) {
    free(tmp);
    return false;
  }
  if (got != PGB_TOTAL_BYTES || tmp[0] != 'P' || tmp[1] != 'G' || tmp[2] != 'B' || tmp[3] != '1') {
    free(tmp);
    setFault("pgb", "bad_header");
    return false;
  }
  uint16_t w = tmp[4] | (tmp[5] << 8);
  uint16_t h = tmp[6] | (tmp[7] << 8);
  uint8_t bpp = tmp[8];
  if (w != PD_W || h != PD_H || bpp != 2) {
    free(tmp);
    setFault("pgb", "bad_geometry");
    return false;
  }
  memcpy(pages[idx].payload, tmp + PGB_HEADER_BYTES, PGB_DATA_BYTES);
  free(tmp);
  pages[idx].revision = revision;
  pages[idx].loaded = true;
  lastPageOkMs = millis();
  Serial.print("Page loaded: "); Serial.print(key); Serial.print(" rev="); Serial.println(revision);
  return true;
}

bool fetchMetaAndChangedPages(bool force) {
  String payload;
  String url = String(relayBaseUrl) + "/api/pd/meta?token=" + relayToken;
  if (!httpGETText(url, payload)) return false;
  DynamicJsonDocument doc(8192);
  DeserializationError err = deserializeJson(doc, payload);
  if (err) {
    setFault("json", String("meta_") + err.c_str());
    return false;
  }
  if (!(doc["ok"] | false)) {
    setFault("json", "meta_not_ok");
    return false;
  }
  bundleRevision = doc["bundle_revision"] | 0;
  alertRevision = doc["alert_revision"] | 0;
  parseEventsFromMeta(doc["events"].as<JsonArray>());
  JsonArray arr = doc["pages"].as<JsonArray>();
  bool allOk = true;
  for (JsonObject p : arr) {
    const char* key = p["key"] | "";
    uint32_t rev = p["revision"] | 0;
    if (!fetchOnePage(key, rev, force)) allOk = false;
  }
  lastMetaOkMs = millis();
  if (allOk && (bundleRevision != lastAckedBundleRevision || alertRevision != lastAckedAlertRevision)) {
    if (httpPOSTAck(bundleRevision)) lastAckedBundleRevision = bundleRevision;
  }
  processEvents(true);
  return allOk;
}

uint8_t pgbLevelAt(const uint8_t* body, int x, int y) {
  uint32_t i = (uint32_t)y * PD_W + x;
  uint8_t packed = body[i / 4];
  uint8_t shift = (3 - (i % 4)) * 2;
  return (packed >> shift) & 0x03;
}

bool levelToBlack(uint8_t level, int x, int y) {
  if (level == 0) return true;
  if (level == 3) return false;
  // Ordered 2x2 dither matrix. level 1 = dark grey, level 2 = light grey.
  static const uint8_t threshold2x2[2][2] = {{0, 2}, {3, 1}};
  uint8_t threshold = threshold2x2[y & 1][x & 1];
  uint8_t blackCount = (level == 1) ? 3 : 1;
  return threshold < blackCount;
}

void drawCurrentPageToDisplay(bool fullRefresh) {
  if (currentPage < 0 || currentPage >= pageCount) currentPage = 0;
  if (!pages[currentPage].loaded || !pages[currentPage].payload) {
    showStatusScreen("PD page missing", pageTitles[currentPage]);
    return;
  }
  displayBusy = true;
  Serial.print("Render page: "); Serial.println(pageKeys[currentPage]);
  display.init(115200, true, 2, false);
  display.setRotation(DISPLAY_ROTATION);
  if (fullRefresh) display.setFullWindow();
  else display.setPartialWindow(0, 0, PD_W, PD_H);
  display.firstPage();
  do {
    display.fillScreen(GxEPD_WHITE);
    const uint8_t* body = pages[currentPage].payload;
    for (int y = 0; y < PD_H; y++) {
      for (int x = 0; x < PD_W; x++) {
        uint8_t level = pgbLevelAt(body, x, y);
        if (levelToBlack(level, x, y)) {
          display.drawPixel(x, y, GxEPD_BLACK);
        }
      }
      if ((y % 10) == 0) delay(1);
    }
  } while (display.nextPage());
  display.hibernate();
  displayBusy = false;
  lastRenderMs = millis();
}

void showStatusScreen(const String& line1, const String& line2) {
  displayBusy = true;
  display.init(115200, true, 2, false);
  display.setRotation(DISPLAY_ROTATION);
  display.setFullWindow();
  display.firstPage();
  do {
    display.fillScreen(GxEPD_WHITE);
    display.setTextColor(GxEPD_BLACK);
    display.setTextSize(2);
    display.setCursor(12, 40);
    display.print("Pocket Device");
    display.setTextSize(1);
    display.setCursor(12, 84);
    display.print(line1);
    display.setCursor(12, 104);
    display.print(line2);
    display.setCursor(12, 132);
    display.print("IP: "); display.print(activeAddress);
    display.setCursor(12, 152);
    display.print("Fault: "); display.print(lastFaultStage);
    display.print(" / "); display.print(lastFaultDetail);
  } while (display.nextPage());
  display.hibernate();
  displayBusy = false;
}

String localWebPage() {
  String html = "<html><body><meta name='viewport' content='width=device-width, initial-scale=1'>";
  html += "<h2>" BUILD_VERSION "</h2>";
  html += "<p>Network: " + activeNetworkName + "<br>Address: " + activeAddress;
  html += "<br>Current page: " + String(pageKeys[currentPage]);
  html += "<br>Bundle revision: " + String(bundleRevision);
  html += "<br>Fault: " + lastFaultStage + " / " + lastFaultDetail;
  html += "<br>HTTP code: " + String(lastHttpCode);
  html += "<br>Last URL: " + lastHttpUrl;
  html += "<br>Free heap: " + String(ESP.getFreeHeap());
  html += "<br>Free PSRAM: " + String(ESP.getFreePsram());
  html += "<br>Alert revision: " + String(alertRevision);
  html += " / acked=" + String(lastAckedAlertRevision);
  html += "<br>Events cached: " + String(eventCount);
  html += "<br>Active alarm key: " + activeAlarmKey + "</p>";
  html += "<ul>";
  for (int i = 0; i < pageCount; i++) {
    html += "<li>" + String(pageKeys[i]) + ": " + String(pages[i].loaded ? "loaded" : "missing") + " rev=" + String(pages[i].revision) + "</li>";
  }
  html += "</ul>";
  html += "<p><a href='/next'>Next page</a> · <a href='/refresh'>Refresh from relay</a></p>";
  html += "</body></html>";
  return html;
}

void handleRoot() { server.send(200, "text/html", localWebPage()); }
void handleNext() {
  if (currentPage == alarmPageIndex) acknowledgeActiveAlarm();
  currentPage = (currentPage + 1) % normalPageCount;
  lastNormalPage = currentPage;
  drawCurrentPageToDisplay(false);
  server.sendHeader("Location", "/"); server.send(303);
}
void handleRefresh() { fetchMetaAndChangedPages(true); drawCurrentPageToDisplay(true); server.sendHeader("Location", "/"); server.send(303); }

void handleButton() {
  static bool wasDown = false;
  static unsigned long downAt = 0;
  static bool longHandled = false;
  bool down = digitalRead(BTN_PAGE) == LOW;
  unsigned long now = millis();

  if (down && !wasDown) {
    downAt = now;
    longHandled = false;
    wasDown = true;
  }
  if (down && wasDown && !longHandled && (now - downAt > 1200UL)) {
    longHandled = true;
    if (now - lastButtonChangeMs > 1000UL) {
      lastButtonChangeMs = now;
      fetchMetaAndChangedPages(true);
      drawCurrentPageToDisplay(true);
    }
  }
  if (!down && wasDown) {
    unsigned long held = now - downAt;
    wasDown = false;
    if (!longHandled && held > 30UL && held < 1000UL) {
      if (now - lastButtonChangeMs > 250UL) {
        lastButtonChangeMs = now;
        if (currentPage == alarmPageIndex) acknowledgeActiveAlarm();
        currentPage = (currentPage + 1) % normalPageCount;
        lastNormalPage = currentPage;
        if (currentPage == 0 && WiFi.status() != WL_CONNECTED && !displayBusy) {
          connectPreferredOrFallback();
        }
        drawCurrentPageToDisplay(false);
      }
    }
  }
}

void setup() {
  Serial.begin(115200);
  delay(400);
  Serial.println();
  Serial.println("BOOT " BUILD_VERSION);
  Serial.print("PSRAM: "); Serial.println(psramFound() ? "YES" : "NO");
  allocatePageBuffers();

  pinMode(BTN_PAGE, INPUT_PULLUP);
  pinMode(HAPTIC_X, OUTPUT);
  pinMode(HAPTIC_Y, OUTPUT);
  pinMode(HAPTIC_Z, OUTPUT);
  hapticOff();
  clearEvents();
  SPI.begin(EPD_SCK, -1, EPD_MOSI, EPD_CS);

  connectPreferredOrFallback();
  if (!usingFallbackAP) syncTimeNow();

  server.on("/", handleRoot);
  server.on("/next", handleNext);
  server.on("/refresh", handleRefresh);
  server.begin();

  bool ok = false;
  if (safeForNetwork()) ok = fetchMetaAndChangedPages(true);
  if (ok && pages[currentPage].loaded) {
    drawCurrentPageToDisplay(true);
  } else {
    showStatusScreen("Relay fetch failed", activeNetworkName + " " + activeAddress);
  }
  lastPollMs = millis();
}

void loop() {
  server.handleClient();
  handleButton();
  processEvents(true);

  if (!usingFallbackAP && WiFi.status() == WL_CONNECTED && !displayBusy && (millis() - lastTimeSyncMs > timeSyncIntervalMs)) {
    syncTimeNow();
  }

  if (!usingFallbackAP && WiFi.status() != WL_CONNECTED && !displayBusy) {
    static unsigned long lastReconnect = 0;
    if (millis() - lastReconnect > 60000UL) {
      lastReconnect = millis();
      connectPreferredOrFallback();
    }
  }

  if (!displayBusy && safeForNetwork() && (millis() - lastPollMs > pollIntervalMs)) {
    lastPollMs = millis();
    bool beforeLoaded = pages[currentPage].loaded;
    uint32_t beforeRev = pages[currentPage].revision;
    fetchMetaAndChangedPages(false);
    if (beforeLoaded && pages[currentPage].loaded && pages[currentPage].revision != beforeRev) {
      drawCurrentPageToDisplay(false);
    }
  }
  delay(1);
}
