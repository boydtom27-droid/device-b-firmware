#include <WiFi.h>
#include <WebServer.h>
#include <Preferences.h>
#include <SPI.h>
#include <ESPmDNS.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>
#include <ArduinoJson.h>
#include <time.h>
#include <GxEPD2_3C.h>
#include <Fonts/FreeMonoBold9pt7b.h>
#include <Fonts/FreeMono9pt7b.h>

#define BUILD_VERSION "DEVICE_B_STABLE_MILESTONE_1_OPTION_B"

////////////////////////////////////////////////////////
// DISPLAY PINS
////////////////////////////////////////////////////////

#define CS   10
#define DC   9
#define RST  8
#define BUSY 7
#define PWR_PIN 21
#define REFRESH_BUTTON 14

GxEPD2_3C<GxEPD2_750c_Z08, GxEPD2_750c_Z08::HEIGHT> display(
  GxEPD2_750c_Z08(CS, DC, RST, BUSY)
);

////////////////////////////////////////////////////////
// WIFI / SERVER
////////////////////////////////////////////////////////

WebServer server(80);
Preferences prefs;

const char* fallbackApSSID = "TaskDevice";
const char* fallbackApPassword = "tasks123";

bool usingFallbackAP = false;
String activeAddress = "";
String activeNetworkName = "";
bool mdnsActive = false;

////////////////////////////////////////////////////////
// RELAY SETTINGS
////////////////////////////////////////////////////////

const char* relayBaseUrl = "https://device-b-relay.onrender.com";
const char* relayToken = "abc123xyz789";

////////////////////////////////////////////////////////
// PREFERRED NETWORKS
////////////////////////////////////////////////////////

struct SavedNetwork {
  const char* ssid;
  const char* password;
};

SavedNetwork preferredNetworks[] = {
  {"Tomspot", "Tom00001"}
};

const int preferredNetworkCount =
  sizeof(preferredNetworks) / sizeof(preferredNetworks[0]);

////////////////////////////////////////////////////////
// DISPLAY LAYOUT
////////////////////////////////////////////////////////

const int taskWidth = 480;
const int imageWidth = 320;
const int statusBarHeight = 0;

int tileHeight = 77;
int progressWidth = 260;

////////////////////////////////////////////////////////
// REFRESH CONTROL
////////////////////////////////////////////////////////

unsigned long lastRefresh = 0;
const unsigned long refreshInterval = 300000UL;   // 5 min

unsigned long lastMetaPoll = 0;
const unsigned long metaPollInterval = 30000UL;   // 30 s

bool refreshinprogress = false;

unsigned long lastTasksVersion = 0;
unsigned long lastIdeasVersion = 0;
unsigned long lastPartnerVersion = 0;
unsigned long lastScheduleVersion = 0;
unsigned long lastModeVersion = 0;

bool pendingDisplayRefresh = false;

////////////////////////////////////////////////////////
// TASK / IDEA / PARTNER / SCHEDULE DATA
////////////////////////////////////////////////////////

struct Task {
  String text;
  String location;
  bool urgent;
  unsigned long createdEpoch;
  unsigned long deadlineEpoch;
  int reissueCount;
};

struct IdeaItem {
  String text;
  bool bold;
};

struct PartnerItem {
  String text;
  bool priority;
};

struct ScheduleItem {
  String label;
  int fromMinutes;
  int toMinutes;
};

Task tasks[6];

IdeaItem ideas[20];
int ideaCount = 0;

PartnerItem partnerItems[12];
int partnerItemCount = 0;

ScheduleItem scheduleItems[4];
int scheduleItemCount = 0;

String rightMode = "ideas";

////////////////////////////////////////////////////////
// TIME
////////////////////////////////////////////////////////

bool timeSynced = false;

////////////////////////////////////////////////////////
// DISPLAY WATCHDOG
////////////////////////////////////////////////////////

bool waitForDisplay()
{
  unsigned long start = millis();
  while (digitalRead(BUSY) == LOW)
  {
    if (millis() - start > 15000)
    {
      display.end();
      delay(500);
      display.init();
      return false;
    }
  }
  return true;
}

////////////////////////////////////////////////////////
// DISPLAY POWER CONTROL
////////////////////////////////////////////////////////

void displayWake()
{
  pinMode(PWR_PIN, OUTPUT);
  digitalWrite(PWR_PIN, HIGH);
  delay(80);
}

void displaySleep()
{
  display.hibernate();
  delay(50);
  digitalWrite(PWR_PIN, LOW);
}

////////////////////////////////////////////////////////
// WIFI LOGIC
////////////////////////////////////////////////////////

bool tryConnectOneNetwork(const char* ssid, const char* password, unsigned long timeoutMs)
{
  if (ssid == nullptr || strlen(ssid) == 0)
    return false;

  WiFi.begin(ssid, password);

  unsigned long start = millis();
  while (millis() - start < timeoutMs)
  {
    if (WiFi.status() == WL_CONNECTED)
      return true;
    delay(250);
  }

  WiFi.disconnect(true, true);
  delay(300);
  return false;
}

void stopMDNS()
{
  if (mdnsActive)
  {
    MDNS.end();
    mdnsActive = false;
  }
}

void startFallbackAP()
{
  stopMDNS();
  WiFi.mode(WIFI_AP);
  WiFi.softAP(fallbackApSSID, fallbackApPassword);
  usingFallbackAP = true;
  activeNetworkName = "AP";
  activeAddress = WiFi.softAPIP().toString();
}

void connectPreferredOrFallback()
{
  WiFi.mode(WIFI_STA);
  WiFi.disconnect(true, true);
  delay(300);

  for (int i = 0; i < preferredNetworkCount; i++)
  {
    if (tryConnectOneNetwork(preferredNetworks[i].ssid, preferredNetworks[i].password, 8000))
    {
      usingFallbackAP = false;
      activeNetworkName = preferredNetworks[i].ssid;
      activeAddress = WiFi.localIP().toString();

      if (MDNS.begin("taskdevice"))
      {
        MDNS.addService("http", "tcp", 80);
        mdnsActive = true;
      }
      return;
    }
  }

  startFallbackAP();
}

////////////////////////////////////////////////////////
// NTP
////////////////////////////////////////////////////////

void syncTime()
{
  if (usingFallbackAP)
  {
    timeSynced = false;
    return;
  }

  configTime(0, 0, "pool.ntp.org", "time.nist.gov");

  struct tm timeinfo;
  for (int i = 0; i < 20; i++)
  {
    if (getLocalTime(&timeinfo))
    {
      timeSynced = true;
      return;
    }
    delay(500);
  }

  timeSynced = false;
}

unsigned long nowEpoch()
{
  time_t now;
  time(&now);
  return (unsigned long)now;
}

int nowMinutesOfDay()
{
  time_t now;
  time(&now);
  struct tm *tmNow = localtime(&now);
  if (!tmNow) return 0;
  return (tmNow->tm_hour * 60) + tmNow->tm_min;
}

////////////////////////////////////////////////////////
// HTTP HELPERS
////////////////////////////////////////////////////////

bool httpGET(String url, String &out)
{
  if (usingFallbackAP) return false;
  if (WiFi.status() != WL_CONNECTED) return false;

  WiFiClientSecure client;
  client.setInsecure();

  HTTPClient http;
  if (!http.begin(client, url))
    return false;

  int code = http.GET();
  if (code != 200)
  {
    http.end();
    return false;
  }

  out = http.getString();
  http.end();
  return true;
}

bool httpPOSTempty(String url)
{
  if (usingFallbackAP) return false;
  if (WiFi.status() != WL_CONNECTED) return false;

  WiFiClientSecure client;
  client.setInsecure();

  HTTPClient http;
  if (!http.begin(client, url))
    return false;

  int code = http.POST("");
  http.end();

  return (code >= 200 && code < 300);
}

////////////////////////////////////////////////////////
// RELAY META / ACK
////////////////////////////////////////////////////////

bool fetchRelayMeta(unsigned long &tasksVersion,
                    unsigned long &ideasVersion,
                    unsigned long &partnerVersion,
                    unsigned long &scheduleVersion,
                    unsigned long &modeVersion,
                    unsigned long &refreshRequested)
{
  String payload;
  String url = String(relayBaseUrl) + "/api/meta?token=" + relayToken;

  if (!httpGET(url, payload))
    return false;

  DynamicJsonDocument doc(1536);
  DeserializationError err = deserializeJson(doc, payload);
  if (err)
    return false;

  tasksVersion = doc["tasks_version"] | 0;
  ideasVersion = doc["ideas_version"] | 0;
  partnerVersion = doc["partner_version"] | 0;
  scheduleVersion = doc["schedule_version"] | 0;
  modeVersion = doc["mode_version"] | 0;
  refreshRequested = doc["refresh_requested"] | 0;
  return true;
}

void ackRelayRefresh()
{
  String url = String(relayBaseUrl) + "/api/ack_refresh?token=" + relayToken;
  httpPOSTempty(url);
}

////////////////////////////////////////////////////////
// SECTION FETCHES
////////////////////////////////////////////////////////

bool fetchTasks()
{
  String payload;
  String url = String(relayBaseUrl) + "/api/tasks?token=" + relayToken;

  if (!httpGET(url, payload))
    return false;

  DynamicJsonDocument doc(4096);
  DeserializationError err = deserializeJson(doc, payload);
  if (err)
    return false;

  for (int i = 0; i < 6; i++)
  {
    tasks[i].text = "";
    tasks[i].location = "";
    tasks[i].urgent = false;
    tasks[i].createdEpoch = 0;
    tasks[i].deadlineEpoch = 0;
    tasks[i].reissueCount = 0;
  }

  JsonArray taskArray = doc["tasks"].as<JsonArray>();
  int idx = 0;
  for (JsonObject t : taskArray)
  {
    if (idx >= 6) break;
    tasks[idx].text = t["text"].as<String>();
    tasks[idx].location = t["location"].as<String>();
    tasks[idx].urgent = t["urgent"].as<bool>();
    tasks[idx].createdEpoch = t["created"].as<unsigned long>();
    tasks[idx].deadlineEpoch = t["deadline"].as<unsigned long>();
    tasks[idx].reissueCount = t["reissueCount"].as<int>();
    idx++;
  }

  return true;
}

bool fetchIdeas()
{
  String payload;
  String url = String(relayBaseUrl) + "/api/ideas?token=" + relayToken;

  if (!httpGET(url, payload))
    return false;

  DynamicJsonDocument doc(4096);
  DeserializationError err = deserializeJson(doc, payload);
  if (err)
    return false;

  ideaCount = 0;
  JsonArray ideaArray = doc["ideas"].as<JsonArray>();
  for (JsonObject idea : ideaArray)
  {
    if (ideaCount >= 20) break;
    ideas[ideaCount].text = idea["text"].as<String>();
    ideas[ideaCount].bold = idea["bold"].as<bool>();
    ideaCount++;
  }

  return true;
}

bool fetchPartnerItems()
{
  String payload;
  String url = String(relayBaseUrl) + "/api/partner?token=" + relayToken;

  if (!httpGET(url, payload))
    return false;

  DynamicJsonDocument doc(4096);
  DeserializationError err = deserializeJson(doc, payload);
  if (err)
    return false;

  partnerItemCount = 0;
  JsonArray partnerArray = doc["partner_items"].as<JsonArray>();
  for (JsonObject item : partnerArray)
  {
    if (partnerItemCount >= 12) break;
    partnerItems[partnerItemCount].text = item["text"].as<String>();
    partnerItems[partnerItemCount].priority = item["priority"].as<bool>();
    partnerItemCount++;
  }

  return true;
}

bool fetchScheduleItems()
{
  String payload;
  String url = String(relayBaseUrl) + "/api/schedule?token=" + relayToken;

  if (!httpGET(url, payload))
    return false;

  DynamicJsonDocument doc(2048);
  DeserializationError err = deserializeJson(doc, payload);
  if (err)
    return false;

  scheduleItemCount = 0;
  JsonArray scheduleArray = doc["schedule_items"].as<JsonArray>();
  for (JsonObject item : scheduleArray)
  {
    if (scheduleItemCount >= 4) break;
    scheduleItems[scheduleItemCount].label = item["label"].as<String>();
    scheduleItems[scheduleItemCount].fromMinutes = item["from_minutes"].as<int>();
    scheduleItems[scheduleItemCount].toMinutes = item["to_minutes"].as<int>();
    scheduleItemCount++;
  }

  return true;
}

bool fetchRightMode()
{
  String payload;
  String url = String(relayBaseUrl) + "/api/mode?token=" + relayToken;

  if (!httpGET(url, payload))
    return false;

  DynamicJsonDocument doc(512);
  DeserializationError err = deserializeJson(doc, payload);
  if (err)
    return false;

  rightMode = doc["right_mode"].as<String>();
  if (rightMode != "ideas" && rightMode != "schedule")
    rightMode = "ideas";

  return true;
}

////////////////////////////////////////////////////////
// TASK DRAWING
////////////////////////////////////////////////////////

float getProgress(Task &t)
{
  if (t.text.length() == 0) return 0;
  if (!timeSynced) return 0;
  if (t.deadlineEpoch <= t.createdEpoch) return 0;

  float p = (float)(nowEpoch() - t.createdEpoch) /
            (float)(t.deadlineEpoch - t.createdEpoch);

  if (p < 0) p = 0;
  if (p > 1) p = 1;
  return p;
}

String getOverdueText(Task &t)
{
  if (t.text.length() == 0) return "";
  if (!timeSynced) return "";
  if (nowEpoch() <= t.deadlineEpoch) return "";

  unsigned long overdueMin = (nowEpoch() - t.deadlineEpoch) / 60UL;
  if (overdueMin < 60) return String(overdueMin) + "min";

  unsigned long overdueHours = overdueMin / 60UL;
  if (overdueHours < 24) return String(overdueHours) + "h";

  unsigned long overdueDays = overdueHours / 24UL;
  return String(overdueDays) + "day";
}

void drawProgressBar(int x, int y, float progress)
{
  display.drawRect(x, y, progressWidth, 10, GxEPD_BLACK);
  int fill = (int)(progressWidth * progress);
  if (fill > 0)
    display.fillRect(x, y, fill, 10, GxEPD_BLACK);
}

void drawReissueBars(int x, int y, int count)
{
  int segments = 5;
  if (count > segments) count = segments;
  if (count < 0) count = 0;

  for (int i = 0; i < segments; i++)
  {
    int sy = y + (segments - i - 1) * 8;
    if (i < count)
      display.fillRect(x, sy, 6, 6, GxEPD_BLACK);
    else
      display.drawRect(x, sy, 6, 6, GxEPD_BLACK);
  }
}

void drawWrappedText(String text, int x, int y)
{
  int maxChars = 22;

  if (text.length() <= maxChars)
  {
    display.setCursor(x, y);
    display.print(text);
    return;
  }

  display.setCursor(x, y);
  display.print(text.substring(0, maxChars));

  display.setCursor(x, y + 20);
  display.print(text.substring(maxChars));
}

void drawUrgentBorder(int x, int y, int w, int h)
{
  display.drawRect(x, y, w, h, GxEPD_RED);
  display.drawRect(x + 1, y + 1, w - 2, h - 2, GxEPD_RED);
}

int tileTop(int index)
{
  return statusBarHeight + index * tileHeight;
}

void drawTile(int i)
{
  int y = tileTop(i);
  Task &t = tasks[i];

  display.drawRect(0, y, taskWidth, tileHeight, GxEPD_BLACK);

  if (t.text.length() > 0)
  {
    if (t.urgent)
      drawUrgentBorder(4, y + 4, 260, 34);

    display.setTextColor(GxEPD_BLACK);
    display.setFont(&FreeMonoBold9pt7b);

    drawWrappedText(t.text, 8, y + 22);

    display.setCursor(300, y + 22);
    display.print(t.location);

    drawProgressBar(8, y + 50, getProgress(t));
    drawReissueBars(400, y + 18, t.reissueCount);

    String overdue = getOverdueText(t);
    if (overdue.length() > 0)
    {
      display.setTextColor(GxEPD_RED);
      display.setCursor(430, y + 40);
      display.print(overdue);
      display.setTextColor(GxEPD_BLACK);
    }
  }
  else
  {
    drawProgressBar(8, y + 50, 0);
  }
}

////////////////////////////////////////////////////////
// RIGHT PANEL
////////////////////////////////////////////////////////

void drawIdeaPanelTop()
{
  display.setTextColor(GxEPD_BLACK);

  int x = taskWidth + 10;
  int y = 24;
  int lineStep = 18;
  int column2x = taskWidth + 165;
  int maxLinesPerCol = 9;

  for (int i = 0; i < ideaCount && i < 18; i++)
  {
    int drawX = (i < maxLinesPerCol) ? x : column2x;
    int drawY = (i < maxLinesPerCol) ? y + i * lineStep : y + (i - maxLinesPerCol) * lineStep;

    if (ideas[i].bold)
      display.setFont(&FreeMonoBold9pt7b);
    else
      display.setFont(&FreeMono9pt7b);

    display.setCursor(drawX, drawY);
    display.print("• ");
    display.print(ideas[i].text);
  }
}

void drawScheduleGapBar(int x, int y, int w, int h, float progress)
{
  display.drawRect(x, y, w, h, GxEPD_BLACK);
  int fill = (int)(w * progress);
  if (fill > 0)
    display.fillRect(x, y, fill, h, GxEPD_BLACK);
}

float getGapProgress(int gapStart, int gapEnd)
{
  if (!timeSynced) return 0.0f;
  if (gapEnd <= gapStart) return 1.0f;

  int nowMins = nowMinutesOfDay();
  float p = (float)(nowMins - gapStart) / (float)(gapEnd - gapStart);

  if (p < 0) p = 0;
  if (p > 1) p = 1;
  return p;
}

String fmtMinutes(int mins)
{
  int hh = mins / 60;
  int mm = mins % 60;
  char buf[6];
  snprintf(buf, sizeof(buf), "%02d:%02d", hh, mm);
  return String(buf);
}

void drawSchedulePanelTop()
{
  display.setTextColor(GxEPD_BLACK);
  display.setFont(&FreeMono9pt7b);

  int baseX = taskWidth + 10;
  int textY = 28;
  int rowGap = 42;
  int barX = taskWidth + 20;
  int barW = 270;
  int barH = 8;

  for (int i = 0; i < scheduleItemCount && i < 4; i++)
  {
    int y = textY + i * rowGap;

    String rowText = fmtMinutes(scheduleItems[i].fromMinutes) + "-" +
                     fmtMinutes(scheduleItems[i].toMinutes) + "  " +
                     scheduleItems[i].label;

    display.setCursor(baseX, y);
    display.print(rowText);

    if (i < scheduleItemCount - 1)
    {
      int gapStart = scheduleItems[i].toMinutes;
      int gapEnd = scheduleItems[i + 1].fromMinutes;
      float gapProgress = (gapEnd <= gapStart) ? 1.0f : getGapProgress(gapStart, gapEnd);

      drawScheduleGapBar(barX, y + 10, barW, barH, gapProgress);
    }
  }
}

void drawPartnerPanelBottom()
{
  int panelTop = 360;

  display.drawLine(taskWidth, panelTop, taskWidth + imageWidth, panelTop, GxEPD_BLACK);
  display.setTextColor(GxEPD_BLACK);

  int x = taskWidth + 10;
  int y = panelTop + 18;
  int lineStep = 18;

  for (int i = 0; i < partnerItemCount && i < 5; i++)
  {
    display.setFont(&FreeMono9pt7b);
    display.setCursor(x, y + i * lineStep);
    display.print("• ");
    display.print(partnerItems[i].text);

    if (partnerItems[i].priority)
    {
      display.setTextColor(GxEPD_RED);
      display.print(" PRIORITY");
      display.setTextColor(GxEPD_BLACK);
    }
  }
}

void drawRightPanel()
{
  display.drawRect(taskWidth, 0, imageWidth, 480, GxEPD_BLACK);

  if (rightMode == "schedule")
    drawSchedulePanelTop();
  else
    drawIdeaPanelTop();

  drawPartnerPanelBottom();
}

////////////////////////////////////////////////////////
// STATUS BAR
////////////////////////////////////////////////////////

void drawStatusBar()
{
  // intentionally blank
}

////////////////////////////////////////////////////////
// RENDERERS
////////////////////////////////////////////////////////

void renderFullScreen()
{
  display.setFullWindow();
  display.firstPage();
  do
  {
    display.fillScreen(GxEPD_WHITE);
    display.setFont(&FreeMonoBold9pt7b);
    display.setTextColor(GxEPD_BLACK);

    drawStatusBar();

    for (int i = 0; i < 6; i++)
      drawTile(i);

    drawRightPanel();

  } while (display.nextPage());
}

void updateDisplayFull()
{
  if (refreshinprogress) return;
  refreshinprogress = true;

  displayWake();
  display.init();
  delay(100);
  waitForDisplay();
  renderFullScreen();
  displaySleep();

  refreshinprogress = false;
}

////////////////////////////////////////////////////////
// LOCAL WEB PAGE
////////////////////////////////////////////////////////

String localWebpage()
{
  String page = "<html><body>";
  page += "<h2>";
  page += BUILD_VERSION;
  page += "</h2>";
  page += "<p>Mode: ";
  page += usingFallbackAP ? "Fallback AP" : "Preferred WiFi";
  page += "<br>Address: ";
  page += activeAddress;
  if (mdnsActive)
    page += "<br>mDNS: taskdevice.local";
  page += "</p>";
  page += "<form action='/refresh'><input type='submit' value='Refresh Device'></form>";
  page += "</body></html>";
  return page;
}

void handleRoot()
{
  server.send(200, "text/html", localWebpage());
}

void handleRefresh()
{
  if (!refreshinprogress)
  {
    pendingDisplayRefresh = true;
  }

  server.sendHeader("Location", "/");
  server.send(303);
}

////////////////////////////////////////////////////////
// SETUP
////////////////////////////////////////////////////////

void setup()
{
  Serial.begin(115200);

  pinMode(PWR_PIN, OUTPUT);
  digitalWrite(PWR_PIN, LOW);

  pinMode(REFRESH_BUTTON, INPUT_PULLUP);

  connectPreferredOrFallback();
  syncTime();

  fetchTasks();
  fetchIdeas();
  fetchPartnerItems();
  fetchScheduleItems();
  fetchRightMode();

  server.on("/", handleRoot);
  server.on("/refresh", handleRefresh);
  server.begin();

  updateDisplayFull();

  lastRefresh = millis();
  lastMetaPoll = millis();
}

////////////////////////////////////////////////////////
// LOOP
////////////////////////////////////////////////////////

void loop()
{
  server.handleClient();

  // 30 s metadata poll only
  if (millis() - lastMetaPoll > metaPollInterval)
  {
    if (!refreshinprogress)
    {
      unsigned long tasksVersion = 0;
      unsigned long ideasVersion = 0;
      unsigned long partnerVersion = 0;
      unsigned long scheduleVersion = 0;
      unsigned long modeVersion = 0;
      unsigned long refreshRequested = 0;

      if (fetchRelayMeta(tasksVersion, ideasVersion, partnerVersion, scheduleVersion, modeVersion, refreshRequested))
      {
        bool anyDataChanged = false;

        if (tasksVersion != lastTasksVersion)
        {
          fetchTasks();
          lastTasksVersion = tasksVersion;
          anyDataChanged = true;
        }

        if (ideasVersion != lastIdeasVersion)
        {
          fetchIdeas();
          lastIdeasVersion = ideasVersion;
          anyDataChanged = true;
        }

        if (partnerVersion != lastPartnerVersion)
        {
          fetchPartnerItems();
          lastPartnerVersion = partnerVersion;
          anyDataChanged = true;
        }

        if (scheduleVersion != lastScheduleVersion)
        {
          fetchScheduleItems();
          lastScheduleVersion = scheduleVersion;
          anyDataChanged = true;
        }

        if (modeVersion != lastModeVersion)
        {
          fetchRightMode();
          lastModeVersion = modeVersion;
          anyDataChanged = true;
        }

        if (anyDataChanged)
          pendingDisplayRefresh = pendingDisplayRefresh; // keep current state; refresh only on schedule or explicit request

        if (refreshRequested)
        {
          pendingDisplayRefresh = true;
          ackRelayRefresh();
        }
      }
    }

    lastMetaPoll = millis();
  }

  // scheduled timed refresh
  if (millis() - lastRefresh > refreshInterval)
  {
    if (!refreshinprogress)
    {
      syncTime();
      updateDisplayFull();
      lastRefresh = millis();
      pendingDisplayRefresh = false;
    }
  }

  // explicit queued refresh from webpage/local button
  if (pendingDisplayRefresh && !refreshinprogress)
  {
    syncTime();
    updateDisplayFull();
    lastRefresh = millis();
    pendingDisplayRefresh = false;
  }

  // physical button refresh
  if (digitalRead(REFRESH_BUTTON) == LOW)
  {
    if (!refreshinprogress)
    {
      syncTime();
      updateDisplayFull();
      delay(1000);
      lastRefresh = millis();
      pendingDisplayRefresh = false;
    }
  }
}
