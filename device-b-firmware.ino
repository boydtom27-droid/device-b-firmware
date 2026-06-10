/*
  Device B Font Demo - Built-in Adafruit GFX fonts
  Purpose: render several candidate firmware fonts on the Waveshare 7.5" B/W/Red display
  without Wi-Fi/relay logic, so visual quality can be compared directly on the panel.

  Board compile suggestion:
    --fqbn "esp32:esp32:esp32s3:FlashSize=16M,PSRAM=opi"

  Current pin map in this file:
    PWR  -> GPIO21
    BUSY -> GPIO7
    DC   -> GPIO9
    CS   -> GPIO10
    RST  -> GPIO8
    CLK  -> GPIO12
    DIN  -> GPIO11
    GND  -> GND
    VCC  -> 3V3

  If you have wired the older map instead, change only the #define block below.
*/

#include <SPI.h>
#include <GxEPD2_3C.h>

#include <Fonts/FreeMono9pt7b.h>
#include <Fonts/FreeMonoBold9pt7b.h>
#include <Fonts/FreeMono12pt7b.h>
#include <Fonts/FreeMonoBold12pt7b.h>
#include <Fonts/FreeSans9pt7b.h>
#include <Fonts/FreeSansBold9pt7b.h>
#include <Fonts/FreeSans12pt7b.h>
#include <Fonts/FreeSansBold12pt7b.h>
#include <Fonts/FreeSerif9pt7b.h>
#include <Fonts/FreeSerifBold9pt7b.h>
#include <Fonts/FreeSerif12pt7b.h>
#include <Fonts/FreeSerifBold12pt7b.h>

// === PIN MAP ===
#define CS 10
#define DC 9
#define RST 8
#define BUSY 7
#define PWR_PIN 21

#define SCLK_PIN 12
#define MOSI_PIN 11

// Waveshare 7.5" B/W/Red V3 class used in the current firmware branch.
GxEPD2_3C<GxEPD2_750c_Z08, GxEPD2_750c_Z08::HEIGHT> display(
  GxEPD2_750c_Z08(CS, DC, RST, BUSY)
);

struct FontRow {
  const char* name;
  const GFXfont* font;
  const char* sample;
};

FontRow rows[] = {
  {"FreeSans 12",       &FreeSans12pt7b,       "16:45-17:15  Sort room  [43%]"},
  {"FreeSans Bold 12",  &FreeSansBold12pt7b,   "16:45-17:15  Sort room  [43%]"},
  {"FreeMono 12",       &FreeMono12pt7b,       "16:45-17:15  Sort room  [43%]"},
  {"FreeMono Bold 12",  &FreeMonoBold12pt7b,   "16:45-17:15  Sort room  [43%]"},
  {"FreeSerif 12",      &FreeSerif12pt7b,      "16:45-17:15  Sort room  [43%]"},
  {"FreeSerif Bold 12", &FreeSerifBold12pt7b,  "16:45-17:15  Sort room  [43%]"},
  {"FreeSans 9",        &FreeSans9pt7b,        "Buy groceries / Bath payment / Calendar"},
  {"FreeSans Bold 9",   &FreeSansBold9pt7b,    "Buy groceries / Bath payment / Calendar"},
  {"FreeMono 9",        &FreeMono9pt7b,        "Graph 0 1 2 3 4 5 6 7 8 9"},
  {"FreeMono Bold 9",   &FreeMonoBold9pt7b,    "Graph 0 1 2 3 4 5 6 7 8 9"},
  {"FreeSerif 9",       &FreeSerif9pt7b,       "Idea bank / partner / scribbles"},
  {"FreeSerif Bold 9",  &FreeSerifBold9pt7b,   "Idea bank / partner / scribbles"}
};

const int rowCount = sizeof(rows) / sizeof(rows[0]);

void powerOnDisplay() {
  pinMode(PWR_PIN, OUTPUT);
  digitalWrite(PWR_PIN, HIGH);
  delay(250);
}

void powerOffDisplay() {
  delay(100);
  digitalWrite(PWR_PIN, LOW);
}

void drawGuideLines() {
  display.drawRect(0, 0, 800, 480, GxEPD_BLACK);
  display.drawLine(0, 58, 800, 58, GxEPD_BLACK);
  display.drawLine(400, 58, 400, 480, GxEPD_BLACK);
  display.drawLine(0, 286, 800, 286, GxEPD_BLACK);

  // Notional task tile and graph labels to judge wall-distance readability.
  display.setFont(&FreeSansBold12pt7b);
  display.setTextColor(GxEPD_BLACK);
  display.setCursor(16, 38);
  display.print("Device B font demo");

  display.setFont(&FreeMono9pt7b);
  display.setTextColor(GxEPD_RED);
  display.setCursor(560, 36);
  display.print("RED small text [23%]");
}

void drawFontRows() {
  int y = 86;
  const int rowStep = 31;

  for (int i = 0; i < rowCount; i++) {
    int columnX = (i < 6) ? 14 : 414;
    int localIndex = (i < 6) ? i : i - 6;
    int rowY = 90 + localIndex * rowStep;

    // Label in small mono so the candidate font itself is visually separate.
    display.setFont(&FreeMono9pt7b);
    display.setTextColor(GxEPD_RED);
    display.setCursor(columnX, rowY);
    display.print(rows[i].name);

    display.setFont(rows[i].font);
    display.setTextColor(GxEPD_BLACK);
    display.setCursor(columnX + 150, rowY);
    display.print(rows[i].sample);
  }
}

void drawLargeComparison() {
  display.setTextColor(GxEPD_BLACK);

  display.setFont(&FreeSansBold12pt7b);
  display.setCursor(18, 330);
  display.print("FreeSansBold12 task title");
  display.setFont(&FreeSans12pt7b);
  display.setCursor(18, 360);
  display.print("Regular supporting line 16:45-17:15");

  display.setFont(&FreeMonoBold12pt7b);
  display.setCursor(18, 408);
  display.print("FreeMonoBold12 task title");
  display.setFont(&FreeMono12pt7b);
  display.setCursor(18, 438);
  display.print("Regular supporting line 16:45-17:15");

  display.setFont(&FreeSerifBold12pt7b);
  display.setCursor(420, 330);
  display.print("FreeSerifBold12 title");
  display.setFont(&FreeSerif12pt7b);
  display.setCursor(420, 360);
  display.print("Regular supporting line");

  display.setFont(&FreeSansBold9pt7b);
  display.setTextColor(GxEPD_RED);
  display.setCursor(420, 408);
  display.print("Red status / overdue / urgent text");

  display.setFont(&FreeSans9pt7b);
  display.setTextColor(GxEPD_BLACK);
  display.setCursor(420, 438);
  display.print("Small axis labels: 0 2 4 6 8 10");
}

void renderFontDemo() {
  display.setRotation(1); // 800 x 480 landscape
  display.setFullWindow();
  display.firstPage();
  do {
    display.fillScreen(GxEPD_WHITE);
    drawGuideLines();
    drawFontRows();
    drawLargeComparison();
  } while (display.nextPage());
}

void setup() {
  Serial.begin(115200);
  delay(300);
  Serial.println();
  Serial.println("BOOT: DEVICE_B_FONT_DEMO_BUILTIN_GFX");

  powerOnDisplay();
  SPI.begin(SCLK_PIN, -1, MOSI_PIN, CS);
  display.init(115200, true, 2, false);
  renderFontDemo();
  display.hibernate();
  Serial.println("FONT DEMO COMPLETE");
}

void loop() {
  delay(10000);
}
