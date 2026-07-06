/*
  Pocket E-paper pin + orientation test
  Target: ESP32-S3 DevKit + Waveshare Pico-ePaper-4.2 400x300 B/W display

  Purpose:
    1) Confirms the selected SPI/control pins are functional.
    2) Confirms the assumed landscape orientation:
       DISPLAY LANDSCAPE, RIBBON/CABLE EDGE FACING THE USER.

  Wiring used by this test:
    Display VCC  -> ESP32 3V3
    Display GND  -> ESP32 GND
    Display DIN  -> ESP32 GPIO11   // MOSI
    Display SCK  -> ESP32 GPIO12   // SPI clock
    Display CS   -> ESP32 GPIO10
    Display DC   -> ESP32 GPIO9
    Display RST  -> ESP32 GPIO8
    Display BUSY -> ESP32 GPIO7

  Optional buttons, not required for the display test:
    BTN_NEXT -> GPIO14 to GND, INPUT_PULLUP
    BTN_PREV -> GPIO13 to GND, INPUT_PULLUP

  Display board setup:
    BS selector should be set to 0 / 4-wire SPI.

  If the display works but text is upside-down with the ribbon edge facing you,
  change TEST_ROTATION from 0 to 2 and reflash.

  If this does not compile because the panel class is unknown in your installed
  GxEPD2 version, try one of the alternative class lines below.
*/

#include <SPI.h>
#include <GxEPD2_BW.h>
#include <Fonts/FreeMonoBold9pt7b.h>
#include <Fonts/FreeMono9pt7b.h>

// ---------------- Pin allocation ----------------
#define EPD_BUSY 7
#define EPD_RST  8
#define EPD_DC   9
#define EPD_CS   10
#define EPD_MOSI 11
#define EPD_SCK  12

#define BTN_NEXT 14
#define BTN_PREV 13

// Set to 0 for the first test. If upside-down with ribbon/cable edge facing you, try 2.
#define TEST_ROTATION 0

// ---------------- Display driver selection ----------------
// Most likely for newer Waveshare 4.2 inch B/W 400x300 modules.
GxEPD2_BW<GxEPD2_420_GDEY042T81, GxEPD2_420_GDEY042T81::HEIGHT> display(
  GxEPD2_420_GDEY042T81(EPD_CS, EPD_DC, EPD_RST, EPD_BUSY)
);

// If the above class does not compile, comment it out and try ONE of these instead:
// GxEPD2_BW<GxEPD2_420, GxEPD2_420::HEIGHT> display(GxEPD2_420(EPD_CS, EPD_DC, EPD_RST, EPD_BUSY));
// GxEPD2_BW<GxEPD2_420_GDEW042T2, GxEPD2_420_GDEW042T2::HEIGHT> display(GxEPD2_420_GDEW042T2(EPD_CS, EPD_DC, EPD_RST, EPD_BUSY));

void drawCornerLabel(int16_t x, int16_t y, const char* label) {
  display.setFont(&FreeMonoBold9pt7b);
  display.setTextColor(GxEPD_BLACK);
  display.setCursor(x, y);
  display.print(label);
}

void drawArrowRight(int x, int y, int len) {
  display.drawLine(x, y, x + len, y, GxEPD_BLACK);
  display.drawLine(x + len, y, x + len - 8, y - 5, GxEPD_BLACK);
  display.drawLine(x + len, y, x + len - 8, y + 5, GxEPD_BLACK);
}

void drawArrowDown(int x, int y, int len) {
  display.drawLine(x, y, x, y + len, GxEPD_BLACK);
  display.drawLine(x, y + len, x - 5, y + len - 8, GxEPD_BLACK);
  display.drawLine(x, y + len, x + 5, y + len - 8, GxEPD_BLACK);
}

void drawPinTable(int x, int y) {
  display.setFont(NULL);
  display.setTextColor(GxEPD_BLACK);

  display.setCursor(x, y);      display.print("DISPLAY -> ESP32-S3");
  display.setCursor(x, y + 14); display.print("BUSY -> GPIO7");
  display.setCursor(x, y + 26); display.print("RST  -> GPIO8");
  display.setCursor(x, y + 38); display.print("DC   -> GPIO9");
  display.setCursor(x, y + 50); display.print("CS   -> GPIO10");
  display.setCursor(x, y + 62); display.print("SCK  -> GPIO12");
  display.setCursor(x, y + 74); display.print("DIN  -> GPIO11 MOSI");
  display.setCursor(x, y + 86); display.print("VCC  -> 3V3, GND -> GND");
}

void drawButtonStatus(int x, int y) {
  display.setFont(NULL);
  display.setTextColor(GxEPD_BLACK);
  display.setCursor(x, y);
  display.print("BTN14=");
  display.print(digitalRead(BTN_NEXT) == LOW ? "LOW/PRESSED" : "HIGH");
  display.setCursor(x, y + 12);
  display.print("BTN13=");
  display.print(digitalRead(BTN_PREV) == LOW ? "LOW/PRESSED" : "HIGH");
}

void drawOrientationPattern() {
  int16_t w = display.width();
  int16_t h = display.height();

  display.fillScreen(GxEPD_WHITE);

  // Outer border and asymmetric internal frame make rotation/mirroring obvious.
  display.drawRect(0, 0, w, h, GxEPD_BLACK);
  display.drawRect(3, 3, w - 6, h - 6, GxEPD_BLACK);
  display.fillRect(0, 0, 34, 34, GxEPD_BLACK);              // black block top-left
  display.drawRect(w - 35, 1, 33, 33, GxEPD_BLACK);         // outlined block top-right
  display.drawLine(0, h - 1, w - 1, 0, GxEPD_BLACK);        // diagonal

  // Corner labels.
  drawCornerLabel(42, 24, "TL");
  drawCornerLabel(w - 72, 24, "TR");
  drawCornerLabel(12, h - 14, "BL");
  drawCornerLabel(w - 72, h - 14, "BR");

  // Main title and orientation statement.
  display.setFont(&FreeMonoBold9pt7b);
  display.setTextColor(GxEPD_BLACK);
  display.setCursor(60, 58);
  display.print("POCKET E-PAPER TEST");

  display.setFont(&FreeMono9pt7b);
  display.setCursor(34, 88);
  display.print("Landscape: ribbon/cable edge faces user");

  // Direction indicators.
  display.setFont(NULL);
  display.setCursor(158, 10);
  display.print("TOP / AWAY FROM USER");
  display.setCursor(126, h - 22);
  display.print("BOTTOM / RIBBON / TOWARDS USER");

  drawArrowRight(34, 112, 105);
  display.setCursor(150, 107);
  display.print("X increases right");

  drawArrowDown(34, 130, 62);
  display.setCursor(48, 160);
  display.print("Y increases down");

  // A simple panel split matching the future pocket concept.
  display.drawRect(214, 104, 166, 70, GxEPD_BLACK);
  display.setCursor(224, 116); display.print("FULL-PAGE PANEL");
  display.setCursor(224, 130); display.print("e.g. right panel");
  display.setCursor(224, 144); display.print("or compact task view");

  display.drawRect(214, 184, 166, 42, GxEPD_BLACK);
  display.setCursor(224, 198); display.print("Rotation = ");
  display.print(TEST_ROTATION);

  drawPinTable(12, 206);
  drawButtonStatus(236, 236);

  display.setFont(NULL);
  display.setCursor(236, 264);
  display.print("If upside-down: set rotation 2");
  display.setCursor(236, 278);
  display.print("If no image: check BS=0 + pins");
}

void setup() {
  Serial.begin(115200);
  delay(500);

  pinMode(BTN_NEXT, INPUT_PULLUP);
  pinMode(BTN_PREV, INPUT_PULLUP);
  pinMode(EPD_BUSY, INPUT);

  Serial.println();
  Serial.println("Pocket e-paper pin/orientation test booting...");
  Serial.println("Expected wiring:");
  Serial.println("  BUSY->7, RST->8, DC->9, CS->10, DIN/MOSI->11, SCK->12, VCC->3V3, GND->GND");
  Serial.println("Expected physical orientation: landscape, ribbon/cable edge facing user.");
  Serial.print("Button GPIO14 state: "); Serial.println(digitalRead(BTN_NEXT));
  Serial.print("Button GPIO13 state: "); Serial.println(digitalRead(BTN_PREV));

  SPI.begin(EPD_SCK, -1, EPD_MOSI, EPD_CS);

  display.init(115200, true, 2, false);
  display.setRotation(TEST_ROTATION);

  display.setFullWindow();
  display.firstPage();
  do {
    drawOrientationPattern();
  } while (display.nextPage());

  display.hibernate();
  Serial.println("Display test pattern sent. Device is now idle.");
}

void loop() {
  delay(1000);
}
