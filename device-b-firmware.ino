/*
  Clip Device (CD) Build 1 C6
  Waveshare ESP32-C6-Zero / Mini + Waveshare 2.13" 250x122 B/W e-paper HAT V4

  Relay endpoints used by this firmware only:
    /api/cd/meta
    /api/cd/page?page=focus|ideas|week
    /api/cd/ack

  ESP32-C6-Zero display connector allocation, chosen to avoid:
    - GPIO8: onboard WS2812 RGB LED
    - GPIO9: BOOT strapping/button pin
    - GPIO12/GPIO13: commonly associated with USB Serial/JTAG on ESP32-C6

    Display VCC  -> 3V3
    Display GND  -> GND
    Display DIN  -> GPIO21  (MOSI)
    Display CLK  -> GPIO20  (SCK)
    Display CS   -> GPIO19
    Display DC   -> GPIO18
    Display RST  -> GPIO5
    Display BUSY -> GPIO4

  Button:
    GPIO14 -> momentary switch -> GND, INPUT_PULLUP
      short press: next page
      long press: force relay refresh

  Optional haptic later:
    GPIO15 -> MOSFET/driver input only. Do not drive a motor directly from GPIO.
    ENABLE_HAPTIC is 0 by default for safety.
*/

#include <WiFi.h>
#include <WebServer.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>
#include <ArduinoJson.h>
#include <SPI.h>
#include <time.h>
#include <GxEPD2_BW.h>

#define BUILD_VERSION "CD_BUILD2_C6_PROMPT_PAGES_EVENT_QUEUE"

// -------- Pins --------
#define EPD_BUSY 4
#define EPD_RST  5
#define EPD_DC   18
#define EPD_CS   19
#define EPD_MOSI 21
#define EPD_SCK  20

#define CD_BUTTON_PIN 14
#define HAPTIC_PIN    15
#define ENABLE_HAPTIC 1

// Set to 1 for landscape. If the image is upside down, change to 3.
#define CD_ROTATION 1

// Waveshare 2.13" HAT V4, 250x122 B/W. If this class is not available in your
// installed GxEPD2 version, try GxEPD2_213_B74 or GxEPD2_213_BN alternatives
// from the GxEPD2 display selection examples.
GxEPD2_BW<GxEPD2_213_BN, GxEPD2_213_BN::HEIGHT> display(
  GxEPD2_213_BN(EPD_CS, EPD_DC, EPD_RST, EPD_BUSY)
);

// -------- Relay / network --------
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

const char* fallbackApSSID = "ClipDevice";
const char* fallbackApPassword = "clip12345";
bool usingFallbackAP = false;
String activeNetworkName = "";
String activeAddress = "";

WebServer server(80);

// -------- CD page cache --------
const int CD_W = 250;
const int CD_H = 122;
const size_t CD_ROW_BYTES = (CD_W + 7) / 8;
const size_t CD_PAGE_BYTES = CD_ROW_BYTES * CD_H;
const size_t CD_CONTIG_PAGE_BYTES = ((CD_W * CD_H) + 7) / 8;
const int CD_PAGE_COUNT = 7;
const int CD_NORMAL_PAGE_COUNT = 6;
const int CD_ALARM_PAGE_INDEX = 6;
const char* CD_PAGE_KEYS[CD_PAGE_COUNT] = {"tasks", "schedule", "ideas_1", "ideas_2", "week_now", "week_next", "alarm"};
const char* CD_PAGE_TITLES[CD_PAGE_COUNT] = {"Tasks", "Today", "Ideas 1", "Ideas 2", "This week", "Next week", "Alarm"};

uint8_t pageCache[CD_PAGE_COUNT][CD_PAGE_BYTES];
bool pageLoaded[CD_PAGE_COUNT] = {false, false, false, false, false, false, false};
uint32_t pageRevision[CD_PAGE_COUNT] = {0, 0, 0, 0, 0, 0, 0};
uint32_t bundleRevision = 0;
uint32_t lastAckedBundleRevision = 0;
uint32_t alertRevision = 0;
uint32_t lastAckedAlertRevision = 0;
int currentPage = 0;
int lastNormalPage = 0;

const int MAX_PATTERN_STEPS = 16;
const int MAX_EVENTS = 20;
const int MAX_FIRED_KEYS = 32;
struct CdEvent {
  bool active;
  bool popUp;
  time_t epoch;
  char key[96];
  char type[16];
  int pattern[MAX_PATTERN_STEPS];
  int patternCount;
};
CdEvent events[MAX_EVENTS];
int eventCount = 0;
char firedKeys[MAX_FIRED_KEYS][96];
int firedKeyPos = 0;
String activeAlarmKey = "";
String alertState = "none";

// Diagnostics.
String lastFaultStage = "none";
String lastFaultDetail = "";
int lastHttpCode = 0;
String lastHttpUrl = "";
unsigned long lastPollMs = 0;
unsigned long lastRenderMs = 0;
unsigned long lastSuccessfulSyncMs = 0;
const unsigned long metaPollIntervalMs = 60000UL;
const unsigned long httpTimeoutMs = 18000UL;

bool timeSynced = false;

// -------- Forward declarations --------
void connectPreferredOrFallback();
bool tryConnectOneNetwork(const char* ssid, const char* password, unsigned long timeoutMs);
bool safeForNetwork();
bool httpGETString(const String& url, String& out);
bool httpGETBinary(const String& url, uint8_t* buf, size_t maxLen, size_t& outLen);
bool httpPOSTEmpty(const String& url);
bool fetchCdMetaAndPages(bool forceAll);
bool fetchCdPage(int idx, const String& url, uint32_t revision);
void drawCurrentPage();
void drawPageBitmap(const uint8_t* bits);
void handleButton();
void processEvents(bool allowDraw);
void runHapticPattern(const int* pattern, int patternCount, const char* label);
void acknowledgeActiveAlarm();
void syncTimeNow();
void setFault(const String& stage, const String& detail);
String localWebpage();

void setFault(const String& stage, const String& detail) {
  lastFaultStage = stage;
  lastFaultDetail = detail;
  Serial.print("FAULT "); Serial.print(stage); Serial.print(": "); Serial.println(detail);
}

bool tryConnectOneNetwork(const char* ssid, const char* password, unsigned long timeoutMs) {
  if (!ssid || strlen(ssid) == 0) return false;
  Serial.print("Trying WiFi: "); Serial.println(ssid);
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);
  unsigned long start = millis();
  while (millis() - start < timeoutMs) {
    if (WiFi.status() == WL_CONNECTED) return true;
    delay(250);
  }
  WiFi.disconnect(true, true);
  delay(250);
  return false;
}

void startFallbackAP() {
  WiFi.mode(WIFI_AP);
  WiFi.softAP(fallbackApSSID, fallbackApPassword);
  usingFallbackAP = true;
  activeNetworkName = "AP";
  activeAddress = WiFi.softAPIP().toString();
}

void connectPreferredOrFallback() {
  usingFallbackAP = false;
  for (int i = 0; i < preferredNetworkCount; i++) {
    if (tryConnectOneNetwork(preferredNetworks[i].ssid, preferredNetworks[i].password, 8000)) {
      activeNetworkName = preferredNetworks[i].ssid;
      activeAddress = WiFi.localIP().toString();
      Serial.print("WiFi OK: "); Serial.println(activeAddress);
      return;
    }
  }
  Serial.println("WiFi failed; starting fallback AP");
  startFallbackAP();
}

bool safeForNetwork() {
  return (!usingFallbackAP && WiFi.status() == WL_CONNECTED);
}

void syncTimeNow() {
  if (!safeForNetwork()) { timeSynced = false; return; }
  setenv("TZ", "GMT0BST,M3.5.0/1,M10.5.0/2", 1);
  tzset();
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
  struct tm info;
  for (int i = 0; i < 16; i++) {
    if (getLocalTime(&info)) {
      timeSynced = true;
      Serial.println("Time sync OK");
      return;
    }
    delay(250);
  }
  timeSynced = false;
  setFault("time", "ntp_failed");
}

bool configureSecure(WiFiClientSecure& client) {
  client.setInsecure();
  client.setHandshakeTimeout(30);
  return true;
}

bool httpGETString(const String& url, String& out) {
  lastHttpUrl = url;
  lastHttpCode = 0;
  if (!safeForNetwork()) { setFault("net", "not_connected"); return false; }
  WiFiClientSecure client;
  configureSecure(client);
  HTTPClient http;
  http.setTimeout(httpTimeoutMs);
  http.setReuse(false);
  if (!http.begin(client, url)) { setFault("http", "begin_failed"); return false; }
  int code = http.GET();
  lastHttpCode = code;
  if (code != 200) {
    setFault("http", String("GET_") + String(code));
    http.end();
    return false;
  }
  out = http.getString();
  http.end();
  lastFaultStage = "none";
  lastFaultDetail = "";
  return true;
}

bool httpGETBinary(const String& url, uint8_t* buf, size_t maxLen, size_t& outLen) {
  outLen = 0;
  lastHttpUrl = url;
  lastHttpCode = 0;
  if (!safeForNetwork()) { setFault("net", "not_connected"); return false; }
  WiFiClientSecure client;
  configureSecure(client);
  HTTPClient http;
  http.setTimeout(httpTimeoutMs);
  http.setReuse(false);
  if (!http.begin(client, url)) { setFault("http", "bin_begin_failed"); return false; }
  int code = http.GET();
  lastHttpCode = code;
  if (code != 200) {
    setFault("http", String("BIN_GET_") + String(code));
    http.end();
    return false;
  }
  int len = http.getSize();
  if (len <= 0 || (size_t)len > maxLen) {
    setFault("http", String("bad_bin_size_") + String(len));
    http.end();
    return false;
  }
  WiFiClient* stream = http.getStreamPtr();
  size_t total = 0;
  unsigned long lastProgress = millis();
  while (total < (size_t)len) {
    int avail = stream->available();
    if (avail > 0) {
      size_t remaining = (size_t)len - total;
      size_t toRead = remaining < (size_t)avail ? remaining : (size_t)avail;
      if (toRead > 512) toRead = 512;
      size_t n = stream->readBytes(buf + total, toRead);
      if (n > 0) { total += n; lastProgress = millis(); }
    } else {
      delay(5);
    }
    if (millis() - lastProgress > 8000UL) break;
  }
  http.end();
  if (total != (size_t)len) {
    setFault("http", "binary_short_read");
    return false;
  }
  outLen = total;
  lastFaultStage = "none";
  lastFaultDetail = "";
  return true;
}


void hapticSet(bool on) {
#if ENABLE_HAPTIC
  digitalWrite(HAPTIC_PIN, on ? HIGH : LOW);
#else
  (void)on;
#endif
}

void hapticOff() { hapticSet(false); }

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
    hapticSet((i % 2) == 0);
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
  if (currentPage == CD_ALARM_PAGE_INDEX && activeAlarmKey.length() > 0) {
    markKeyFired(activeAlarmKey.c_str());
    Serial.print("ALARM DISMISS "); Serial.println(activeAlarmKey);
  }
  activeAlarmKey = "";
  hapticOff();
}

void processEvents(bool allowDraw) {
  if (!timeSynced) return;
  time_t nowT;
  time(&nowT);
  for (int i = 0; i < eventCount; i++) {
    CdEvent &ev = events[i];
    if (!ev.active || ev.epoch <= 0) continue;
    if (nowT < ev.epoch) continue;
    if (keyWasFired(ev.key)) continue;
    bool isAlert = strcmp(ev.type, "alert") == 0;
    if (ev.popUp && isAlert && allowDraw && pageLoaded[CD_ALARM_PAGE_INDEX]) {
      if (currentPage != CD_ALARM_PAGE_INDEX && currentPage < CD_NORMAL_PAGE_COUNT) lastNormalPage = currentPage;
      currentPage = CD_ALARM_PAGE_INDEX;
      activeAlarmKey = String(ev.key);
      drawCurrentPage();
    }
    runHapticPattern(ev.pattern, ev.patternCount, ev.key);
    markKeyFired(ev.key);
    break;
  }
}


bool httpPOSTEmpty(const String& url) {
  lastHttpUrl = url;
  lastHttpCode = 0;
  if (!safeForNetwork()) { setFault("net", "not_connected"); return false; }
  WiFiClientSecure client;
  configureSecure(client);
  HTTPClient http;
  http.setTimeout(httpTimeoutMs);
  http.setReuse(false);
  if (!http.begin(client, url)) { setFault("http", "post_begin_failed"); return false; }
  int code = http.POST("");
  lastHttpCode = code;
  http.end();
  if (code < 200 || code >= 300) {
    setFault("http", String("POST_") + String(code));
    return false;
  }
  return true;
}

void unpackContiguousBitsToRowPadded(const uint8_t* src, uint8_t* dst) {
  memset(dst, 0, CD_PAGE_BYTES);
  for (int y = 0; y < CD_H; y++) {
    for (int x = 0; x < CD_W; x++) {
      size_t srcBit = (size_t)y * (size_t)CD_W + (size_t)x;
      uint8_t srcMask = 0x80 >> (srcBit % 8);
      if (src[srcBit / 8] & srcMask) {
        size_t dstIndex = (size_t)y * CD_ROW_BYTES + (size_t)(x / 8);
        dst[dstIndex] |= (0x80 >> (x % 8));
      }
    }
  }
}

bool fetchCdPage(int idx, const String& url, uint32_t revision) {
  if (idx < 0 || idx >= CD_PAGE_COUNT) return false;
  const size_t MAX_PAYLOAD = 10 + CD_PAGE_BYTES;
  const size_t ROW_PADDED_PAYLOAD = 10 + CD_PAGE_BYTES;
  const size_t CONTIG_PAYLOAD = 10 + CD_CONTIG_PAGE_BYTES;
  uint8_t tmp[MAX_PAYLOAD];
  size_t got = 0;
  if (!httpGETBinary(url, tmp, MAX_PAYLOAD, got)) return false;
  if (got != ROW_PADDED_PAYLOAD && got != CONTIG_PAYLOAD) {
    setFault("page", String("bad_length_") + String(got));
    return false;
  }
  if (tmp[0] != 'C' || tmp[1] != 'D' || tmp[2] != 'B' || tmp[3] != '1') {
    setFault("page", "bad_magic"); return false;
  }
  int w = tmp[4] | (tmp[5] << 8);
  int h = tmp[6] | (tmp[7] << 8);
  int bpp = tmp[8];
  if (w != CD_W || h != CD_H || bpp != 1) {
    setFault("page", "bad_header"); return false;
  }

  if (got == ROW_PADDED_PAYLOAD) {
    memcpy(pageCache[idx], tmp + 10, CD_PAGE_BYTES);
    Serial.print("Loaded row-padded page ");
  } else {
    unpackContiguousBitsToRowPadded(tmp + 10, pageCache[idx]);
    Serial.print("Loaded legacy contiguous page ");
  }
  Serial.print(CD_PAGE_KEYS[idx]); Serial.print(" rev "); Serial.println(revision);
  pageLoaded[idx] = true;
  pageRevision[idx] = revision;
  return true;
}

bool fetchCdMetaAndPages(bool forceAll) {
  String metaPayload;
  String url = String(relayBaseUrl) + "/api/cd/meta?token=" + relayToken;
  if (!httpGETString(url, metaPayload)) return false;

  DynamicJsonDocument doc(8192);
  DeserializationError err = deserializeJson(doc, metaPayload);
  if (err) { setFault("json", String("meta_") + err.c_str()); return false; }
  if (!(doc["ok"] | false)) { setFault("json", "meta_not_ok"); return false; }

  bundleRevision = doc["bundle_revision"] | 0;
  alertRevision = doc["alert_revision"] | 0;
  alertState = doc["alert"]["state"] | "none";
  parseEventsFromMeta(doc["events"].as<JsonArray>());

  JsonArray pages = doc["pages"].as<JsonArray>();
  bool anyLoaded = false;
  for (JsonObject p : pages) {
    String key = p["page"] | "";
    String pageUrl = p["url"] | "";
    uint32_t rev = p["revision"] | 0;
    int idx = -1;
    for (int i = 0; i < CD_PAGE_COUNT; i++) {
      if (key == CD_PAGE_KEYS[i]) { idx = i; break; }
    }
    if (idx < 0 || pageUrl.length() == 0) continue;
    if (forceAll || !pageLoaded[idx] || pageRevision[idx] != rev) {
      if (fetchCdPage(idx, pageUrl, rev)) anyLoaded = true;
    }
  }
  lastSuccessfulSyncMs = millis();
  String ackUrl = String(relayBaseUrl) + "/api/cd/ack?token=" + relayToken + "&bundle_revision=" + String(bundleRevision) + "&alert_revision=" + String(alertRevision) + "&page=" + CD_PAGE_KEYS[currentPage];
  if (httpPOSTEmpty(ackUrl)) {
    lastAckedBundleRevision = bundleRevision;
    lastAckedAlertRevision = alertRevision;
  }
  processEvents(true);
  return true;
}

void drawPageBitmap(const uint8_t* bits) {
  // bits are row-padded: CD_ROW_BYTES per row. This is the format expected by Adafruit_GFX drawBitmap().
  display.drawBitmap(0, 0, bits, CD_W, CD_H, GxEPD_BLACK);
}

void drawCurrentPage() {
  if (!pageLoaded[currentPage]) return;
  Serial.print("Drawing page "); Serial.println(CD_PAGE_KEYS[currentPage]);
  display.setRotation(CD_ROTATION);
  display.setFullWindow();
  display.firstPage();
  do {
    display.fillScreen(GxEPD_WHITE);
    drawPageBitmap(pageCache[currentPage]);
  } while (display.nextPage());
  lastRenderMs = millis();
}

void handleButton() {
  static bool wasDown = false;
  static unsigned long downAt = 0;
  bool down = digitalRead(CD_BUTTON_PIN) == LOW;
  if (down && !wasDown) {
    wasDown = true;
    downAt = millis();
  } else if (!down && wasDown) {
    unsigned long held = millis() - downAt;
    wasDown = false;
    if (held > 850) {
      Serial.println("Button: long press refresh");
      if (safeForNetwork()) {
        fetchCdMetaAndPages(true);
      } else {
        connectPreferredOrFallback();
        if (safeForNetwork()) { syncTimeNow(); fetchCdMetaAndPages(true); }
      }
      drawCurrentPage();
    } else if (held > 35) {
      if (currentPage == CD_ALARM_PAGE_INDEX) {
        Serial.println("Button: dismiss alarm");
        acknowledgeActiveAlarm();
        currentPage = lastNormalPage;
      } else {
        Serial.println("Button: short press next page");
        currentPage = (currentPage + 1) % CD_NORMAL_PAGE_COUNT;
      }
      drawCurrentPage();
    }
  }
}

String localWebpage() {
  String p = "<html><body><meta name='viewport' content='width=device-width, initial-scale=1'>";
  p += "<h2>"; p += BUILD_VERSION; p += "</h2>";
  p += "<p>Mode: "; p += usingFallbackAP ? "Fallback AP" : "WiFi";
  p += "<br>Network: "; p += activeNetworkName;
  p += "<br>Address: "; p += activeAddress;
  p += "<br>Page: "; p += CD_PAGE_KEYS[currentPage];
  p += "<br>Bundle rev: "; p += String(bundleRevision);
  p += "<br>Alert rev: "; p += String(alertRevision);
  p += "<br>Events: "; p += String(eventCount);
  p += "<br>Alert state: "; p += alertState;
  p += "<br>Time synced: "; p += timeSynced ? "yes" : "no";
  p += "</p><h3>Diagnostics</h3><p>Fault: "; p += lastFaultStage; p += " / "; p += lastFaultDetail;
  p += "<br>HTTP code: "; p += String(lastHttpCode);
  p += "<br>Last URL: "; p += lastHttpUrl;
  p += "<br>Free heap: "; p += String(ESP.getFreeHeap());
  p += "<br>Chip model: "; p += String(ESP.getChipModel());
  p += "</p>";
  p += "<p>Loaded: ";
  for (int i = 0; i < CD_PAGE_COUNT; i++) { p += CD_PAGE_KEYS[i]; p += pageLoaded[i] ? " ok " : " no "; }
  p += "</p>";
  p += "<form action='/next'><input type='submit' value='Next page'></form>";
  p += "<form action='/refresh'><input type='submit' value='Refresh from relay'></form>";
  p += "</body></html>";
  return p;
}

void handleRoot() { server.send(200, "text/html", localWebpage()); }
void handleNext() { if (currentPage == CD_ALARM_PAGE_INDEX) acknowledgeActiveAlarm(); currentPage = (currentPage + 1) % CD_NORMAL_PAGE_COUNT; drawCurrentPage(); server.sendHeader("Location", "/"); server.send(303); }
void handleRefresh() { if (safeForNetwork()) fetchCdMetaAndPages(true); drawCurrentPage(); server.sendHeader("Location", "/"); server.send(303); }

void setup() {
  Serial.begin(115200);
  delay(400);
  Serial.println();
  Serial.println("BOOT " BUILD_VERSION);

  pinMode(CD_BUTTON_PIN, INPUT_PULLUP);
  pinMode(HAPTIC_PIN, OUTPUT);
  digitalWrite(HAPTIC_PIN, LOW);

  SPI.begin(EPD_SCK, -1, EPD_MOSI, EPD_CS);

  connectPreferredOrFallback();
  server.on("/", handleRoot);
  server.on("/next", handleNext);
  server.on("/refresh", handleRefresh);
  server.begin();
  Serial.println("Local web server started");

  display.init(115200, true, 2, false);
  display.setRotation(CD_ROTATION);

  if (safeForNetwork()) {
    syncTimeNow();
    fetchCdMetaAndPages(true);
  }

  bool any = false;
  for (int i = 0; i < CD_PAGE_COUNT; i++) any = any || pageLoaded[i];
  if (any) {
    currentPage = 0;
    drawCurrentPage();
  } else {
    // Simple local fallback screen if relay unavailable.
    display.setRotation(CD_ROTATION);
    display.setFullWindow();
    display.firstPage();
    do {
      display.fillScreen(GxEPD_WHITE);
      display.setTextColor(GxEPD_BLACK);
      display.setCursor(4, 18);
      display.print("CD relay unavailable");
      display.setCursor(4, 36);
      display.print(activeAddress);
    } while (display.nextPage());
  }

  lastPollMs = millis();
}

void loop() {
  server.handleClient();
  handleButton();
  processEvents(true);

  if (safeForNetwork() && millis() - lastPollMs > metaPollIntervalMs) {
    if (fetchCdMetaAndPages(false)) {
      // Only redraw automatically if the currently visible page changed.
      if (pageLoaded[currentPage]) drawCurrentPage();
    }
    lastPollMs = millis();
  }

  if (!safeForNetwork() && !usingFallbackAP && WiFi.status() != WL_CONNECTED) {
    connectPreferredOrFallback();
  }

  delay(5);
}
