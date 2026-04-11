
import json
import math
import os
import sqlite3
import time
from datetime import datetime
from flask import Flask, request, redirect, jsonify, render_template_string, abort, send_file

app = Flask(__name__)

DB_PATH = os.environ.get("DB_PATH", "tasks.db")
APP_TOKEN = os.environ.get("APP_TOKEN", "abc123xyz789")
PARTNER_TOKEN = os.environ.get("PARTNER_TOKEN", "abc123xyz789-partner")
FIRMWARE_VERSION = os.environ.get("FIRMWARE_VERSION", "3.2.1")
FIRMWARE_PATH = os.environ.get("FIRMWARE_PATH", "device_b.bin")

PAGE = """
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Device B Relay</title>
<style>
body { font-family: sans-serif; margin: 20px; max-width: 980px; }
input[type=text], input[type=datetime-local], input[type=time], input[type=number], textarea, select {
  width: 100%; padding: 8px; margin: 6px 0 12px;
  box-sizing: border-box;
}
textarea { min-height: 120px; font-family: monospace; }
.task, .idea, .partner, .schedule, .graphcard {
  margin: 8px 0; padding: 8px; border: 1px solid #ccc;
}
.links a { margin-right: 12px; }
.urgent { color: #b00; font-weight: bold; }
.priority { color: #b00; font-weight: bold; }
button, input[type=submit] { padding: 10px 14px; }
.muted { color: #666; font-size: 0.9em; }
h3 { margin-top: 28px; }
.grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
code { white-space: pre-wrap; }
small { color: #666; }
</style>
</head>
<body>
<h2>Device B Relay</h2>
<p class="muted"><strong>Note:</strong> avoid patient-identifiable data.</p>

<h3>Displayed Page</h3>
<form action="/set_page" method="post">
<input type="hidden" name="token" value="{{ token }}">
<label><input type="radio" name="page_type" value="main" {% if current_page_type == "main" %}checked{% endif %}> Main page</label><br>
<label><input type="radio" name="page_type" value="graph" {% if current_page_type == "graph" %}checked{% endif %}> Graph page</label><br><br>
<input type="submit" value="Set Displayed Page">
</form>

<h3>Right Panel Mode (main page)</h3>
<form action="/set_mode" method="post">
<input type="hidden" name="token" value="{{ token }}">
<label><input type="radio" name="mode" value="ideas" {% if right_mode == "ideas" %}checked{% endif %}> Idea bank</label><br>
<label><input type="radio" name="mode" value="schedule" {% if right_mode == "schedule" %}checked{% endif %}> Schedule</label><br><br>
<input type="submit" value="Set Mode">
</form>

<div class="grid2">
<div>
<h3>Current Tasks</h3>
{% for t in tasks %}
<div class="task">
<div><strong>{{ t["text"] }}</strong> {% if t["urgent"] %}<span class="urgent">URGENT</span>{% endif %}</div>
<div>{{ t["location"] }}</div>
<div>Deadline: {{ t["deadline_str"] }}</div>
<div>Reissues: {{ t["reissue_count"] }}</div>
<div class="links">
<a href="/delete?id={{ t['id'] }}&token={{ token }}">done</a>
<a href="/reissue?id={{ t['id'] }}&token={{ token }}">reissue</a>
</div>
</div>
{% else %}
<p>No active tasks.</p>
{% endfor %}

<h3>Add Task</h3>
<form action="/add" method="post">
<input type="hidden" name="token" value="{{ token }}">
<label>Task</label>
<input type="text" name="text" required>
<label>Location</label>
<input type="text" name="location" required>
<label>Deadline date/time</label>
<input type="datetime-local" name="deadline_local" required>
<label><input type="checkbox" name="urgent"> Urgent</label><br><br>
<input type="submit" value="Add Task">
</form>

<h3>Partner Input</h3>
{% for item in partner_items %}
<div class="partner">
<div>{{ item["text"] }} {% if item["priority"] %}<span class="priority">PRIORITY</span>{% endif %}</div>
<div class="links">
<a href="/remove_partner?id={{ item['id'] }}&token={{ token }}">remove</a>
</div>
</div>
{% else %}
<p>No partner items.</p>
{% endfor %}
</div>

<div>
{% if right_mode == "ideas" %}
<h3>Idea Bank</h3>
{% for idea in ideas %}
<div class="idea">
<div>{% if idea["bold"] %}<strong>• {{ idea["text"] }}</strong>{% else %}• {{ idea["text"] }}{% endif %}</div>
<div class="links"><a href="/remove_idea?id={{ idea['id'] }}&token={{ token }}">remove</a></div>
</div>
{% else %}
<p>No ideas saved.</p>
{% endfor %}

<h3>Add Idea</h3>
<form action="/add_idea" method="post">
<input type="hidden" name="token" value="{{ token }}">
<label>Idea</label>
<input type="text" name="text" required>
<label><input type="checkbox" name="bold"> Bold</label><br><br>
<input type="submit" value="Add Idea">
</form>
{% endif %}

{% if right_mode == "schedule" %}
<h3>Schedule</h3>
{% for row in schedule_rows %}
<div class="schedule">
<div><strong>{{ row["from_time"] }}-{{ row["to_time"] }}</strong> — {{ row["label"] }}</div>
<div class="links"><a href="/remove_schedule?id={{ row['id'] }}&token={{ token }}">remove</a></div>
</div>
{% else %}
<p>No schedule entries.</p>
{% endfor %}

<h3>Add Schedule Item</h3>
<form action="/add_schedule" method="post">
<input type="hidden" name="token" value="{{ token }}">
<label>Location / Activity</label>
<input type="text" name="label" required>
<label>From</label>
<input type="time" name="from_time" required>
<label>To</label>
<input type="time" name="to_time" required>
<input type="submit" value="Add Schedule Item">
</form>
{% endif %}

{% if current_page_type == "graph" %}
<h3>Graph Page Settings</h3>
<div class="graphcard">
<form action="/graph_settings" method="post">
<input type="hidden" name="token" value="{{ token }}">
<label>Number of graphs</label>
<select name="graph_mode">
<option value="1" {% if graph_mode == 1 %}selected{% endif %}>1 graph</option>
<option value="2" {% if graph_mode == 2 %}selected{% endif %}>2 graphs</option>
</select>

<label>Graph 1 title</label>
<input type="text" name="graph1_label" value="{{ graph1_label }}">

<label>Graph 1 Y-axis range</label>
<select name="graph1_axis_mode">
<option value="auto" {% if graph1_axis_mode == "auto" %}selected{% endif %}>Auto</option>
<option value="manual" {% if graph1_axis_mode == "manual" %}selected{% endif %}>Manual</option>
</select>
<div class="grid2">
<div>
<label>Graph 1 Y min</label>
<input type="number" step="any" name="graph1_y_min" value="{{ graph1_y_min }}">
</div>
<div>
<label>Graph 1 Y max</label>
<input type="number" step="any" name="graph1_y_max" value="{{ graph1_y_max }}">
</div>
</div>

<label><input type="checkbox" name="graph1_avg" {% if graph1_avg %}checked{% endif %}> Show 3-point running average (graph 1)</label><br><br>

<label>Graph 1 data (one point per line: x_label, y_value)</label>
<textarea name="graph1_csv">{{ graph1_csv }}</textarea>

<label>Graph 2 title</label>
<input type="text" name="graph2_label" value="{{ graph2_label }}">

<label>Graph 2 Y-axis range</label>
<select name="graph2_axis_mode">
<option value="auto" {% if graph2_axis_mode == "auto" %}selected{% endif %}>Auto</option>
<option value="manual" {% if graph2_axis_mode == "manual" %}selected{% endif %}>Manual</option>
</select>
<div class="grid2">
<div>
<label>Graph 2 Y min</label>
<input type="number" step="any" name="graph2_y_min" value="{{ graph2_y_min }}">
</div>
<div>
<label>Graph 2 Y max</label>
<input type="number" step="any" name="graph2_y_max" value="{{ graph2_y_max }}">
</div>
</div>

<label><input type="checkbox" name="graph2_avg" {% if graph2_avg %}checked{% endif %}> Show 3-point running average (graph 2)</label><br><br>

<label>Graph 2 data (one point per line: x_label, y_value)</label>
<textarea name="graph2_csv">{{ graph2_csv }}</textarea>
<small>X labels are shared in order; only the bottom graph shows them in dual-graph mode. Vertical stacked y labels are shown to the left of the axis.</small><br><br>

<input type="submit" value="Save Graph Settings">
</form>
</div>
{% endif %}
</div>
</div>

<h3>Device Controls</h3>
<form action="/refresh_now" method="post" style="display:inline-block;">
<input type="hidden" name="token" value="{{ token }}">
<input type="submit" value="Refresh Device">
</form>
<form action="/force_update" method="post" style="display:inline-block; margin-left: 12px;">
<input type="hidden" name="token" value="{{ token }}">
<input type="submit" value="Force OTA Update">
</form>

<hr>
<p class="muted">Partner page: <code>/partner?token=PARTNER_TOKEN</code></p>
<p class="muted">Firmware served at: <code>/firmware/device_b.bin</code></p>
</body>
</html>
"""

PARTNER_PAGE = """
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Partner Input</title>
<style>
body { font-family: sans-serif; margin: 20px; max-width: 520px; }
input[type=text] { width: 100%; padding: 8px; margin: 6px 0 12px; }
input[type=submit] { padding: 10px 14px; }
.muted { color: #666; font-size: 0.9em; }
</style>
</head>
<body>
<h2>Partner Input</h2>
<p class="muted">Simple input only.</p>
<form action="/partner_add" method="post">
<input type="hidden" name="token" value="{{ token }}">
<label>Text</label>
<input type="text" name="text" required>
<label><input type="checkbox" name="priority"> Priority</label><br><br>
<input type="submit" value="Send">
</form>
</body>
</html>
"""


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def require_token(token: str):
    if token != APP_TOKEN:
        abort(403)


def require_partner_token(token: str):
    if token != PARTNER_TOKEN:
        abort(403)


def get_setting(key: str, default: str = ""):
    conn = db()
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    if row is None:
        return default
    return row["value"]


def set_setting(key: str, value: str):
    conn = db()
    conn.execute(
        """
        INSERT INTO settings (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )
    conn.commit()
    conn.close()


def bump_version(name: str):
    current = int(get_setting(name, "1")) + 1
    set_setting(name, str(current))
    set_setting("last_updated", str(int(time.time())))


def bump_page_revision():
    current = int(get_setting("page_revision", "1")) + 1
    set_setting("page_revision", str(current))
    set_setting("last_updated", str(int(time.time())))


def mark_refresh_needed():
    set_setting("refresh_requested", "1")
    set_setting("last_updated", str(int(time.time())))


def deadline_str(epoch_val: int):
    try:
        return datetime.fromtimestamp(epoch_val).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(epoch_val)


def parse_time_to_minutes(s: str):
    hh, mm = s.split(":")
    return int(hh) * 60 + int(mm)


def minutes_to_time_str(m: int):
    hh = m // 60
    mm = m % 60
    return f"{hh:02d}:{mm:02d}"


def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def safe_float_setting(key: str, default=0.0):
    return safe_float(get_setting(key, str(default)), default)


def parse_graph_csv(raw: str):
    points = []
    for line in (raw or "").splitlines():
        line = line.strip()
        if not line or "," not in line:
            continue
        x_label, y_value = line.split(",", 1)
        x_label = x_label.strip()
        y = safe_float(y_value.strip(), None)
        if x_label and y is not None:
            points.append({"x_label": x_label[:18], "y": y})
    return points[:24]


def color_name(name: str):
    return "red" if name == "red" else "black"


def add_text_op(ops, x, y, text, font="mono", color="black"):
    ops.append({"op": "text", "x": int(x), "y": int(y), "text": str(text)[:95], "font": font, "color": color_name(color)})


def add_line_op(ops, x1, y1, x2, y2, color="black"):
    ops.append({"op": "line", "x1": int(x1), "y1": int(y1), "x2": int(x2), "y2": int(y2), "color": color_name(color)})


def add_rect_op(ops, x, y, w, h, color="black", fill=False):
    ops.append({"op": "fill_rect" if fill else "rect", "x": int(x), "y": int(y), "w": int(w), "h": int(h), "color": color_name(color)})


def add_cross_op(ops, x, y, color="black"):
    ops.append({"op": "cross", "x": int(x), "y": int(y), "color": color_name(color)})


def add_stacked_label(ops, x, top_y, label, step=12, font="mono", color="black"):
    for i, ch in enumerate((label or "")[:14]):
        add_text_op(ops, x, top_y + i * step, ch, font, color)


def build_main_render_job():
    conn = db()
    tasks = conn.execute("SELECT * FROM tasks WHERE active = 1 ORDER BY deadline ASC LIMIT 6").fetchall()
    ideas = conn.execute("SELECT * FROM ideas WHERE active = 1 ORDER BY id ASC LIMIT 20").fetchall()
    partner_items = conn.execute("SELECT * FROM partner_items WHERE active = 1 ORDER BY created ASC LIMIT 12").fetchall()
    schedule_rows = conn.execute("SELECT * FROM schedule_items WHERE active = 1 ORDER BY from_minutes ASC LIMIT 4").fetchall()
    conn.close()

    task_width = 480
    image_width = 320
    tile_height = 77
    progress_width = 260
    ops = [{"op": "clear", "color": "white"}]

    now = int(time.time())
    right_mode = get_setting("right_mode", "ideas")

    for i in range(6):
        y = i * tile_height
        add_rect_op(ops, 0, y, task_width, tile_height, "black", fill=False)
        if i < len(tasks):
            t = tasks[i]
            text = t["text"] or ""
            location = t["location"] or ""
            if bool(t["urgent"]):
                ops.append({"op": "urgent_border", "x": 4, "y": y + 4, "w": 260, "h": 34})
            max_chars = 22
            if len(text) <= max_chars:
                add_text_op(ops, 8, y + 22, text, "bold", "black")
            else:
                add_text_op(ops, 8, y + 22, text[:max_chars], "bold", "black")
                add_text_op(ops, 8, y + 42, text[max_chars:max_chars * 2], "bold", "black")
            add_text_op(ops, 300, y + 22, location[:18], "bold", "black")

            created = int(t["created"])
            deadline = int(t["deadline"])
            progress = 0.0
            if deadline > created:
                progress = max(0.0, min(1.0, (now - created) / float(deadline - created)))
            ops.append({"op": "bar_outline", "x": 8, "y": y + 50, "w": progress_width, "h": 10, "color": "black"})
            ops.append({"op": "bar_fill", "x": 8, "y": y + 50, "w": int(progress_width * progress), "h": 10, "color": "black"})
            ops.append({"op": "progress_meta", "x": 8, "y": y + 50, "w": progress_width, "h": 10, "created": created, "deadline": deadline})
            ops.append({"op": "reissue_bars", "x": 400, "y": y + 18, "count": max(0, min(5, int(t["reissue_count"])))})

            overdue = ""
            if now > deadline:
                overdue_min = (now - deadline) // 60
                overdue = f"{overdue_min}m" if overdue_min < 60 else (f"{overdue_min // 60}h" if overdue_min < 1440 else f"{overdue_min // 1440}d")
            if overdue:
                add_text_op(ops, 426, y + 40, overdue[:4], "mono", "red")
        else:
            ops.append({"op": "bar_outline", "x": 8, "y": y + 50, "w": progress_width, "h": 10, "color": "black"})

    add_rect_op(ops, task_width, 0, image_width, 480, "black", fill=False)

    if right_mode == "schedule":
        base_x = task_width + 10
        text_y = 28
        row_gap = 42
        bar_x = task_width + 20
        bar_w = 270
        bar_h = 8
        for i, row in enumerate(schedule_rows[:4]):
            y = text_y + i * row_gap
            row_text = f"{minutes_to_time_str(row['from_minutes'])}-{minutes_to_time_str(row['to_minutes'])} {row['label']}"
            add_text_op(ops, base_x, y, row_text[:42], "mono", "black")
            if i < len(schedule_rows) - 1:
                gap_start = int(schedule_rows[i]["to_minutes"])
                gap_end = int(schedule_rows[i + 1]["from_minutes"])
                progress = 1.0
                if gap_end > gap_start:
                    dt = datetime.fromtimestamp(now)
                    now_mins = dt.hour * 60 + dt.minute
                    progress = max(0.0, min(1.0, (now_mins - gap_start) / float(gap_end - gap_start)))
                ops.append({"op": "bar_outline", "x": bar_x, "y": y + 10, "w": bar_w, "h": bar_h, "color": "black"})
                ops.append({"op": "bar_fill", "x": bar_x, "y": y + 10, "w": int(bar_w * progress), "h": bar_h, "color": "black"})
                ops.append({"op": "schedule_progress_meta", "x": bar_x, "y": y + 10, "w": bar_w, "h": bar_h, "gap_start": gap_start, "gap_end": gap_end})
    else:
        x = task_width + 10
        y = 24
        line_step = 18
        column2x = task_width + 165
        max_lines_per_col = 9
        for i, idea in enumerate(ideas[:18]):
            draw_x = x if i < max_lines_per_col else column2x
            draw_y = y + i * line_step if i < max_lines_per_col else y + (i - max_lines_per_col) * line_step
            add_text_op(ops, draw_x, draw_y, "• " + idea["text"], "bold" if idea["bold"] else "mono", "black")

    panel_top = 360
    add_line_op(ops, task_width, panel_top, task_width + image_width, panel_top, "black")
    x = task_width + 10
    y = panel_top + 18
    line_step = 18
    for i, item in enumerate(partner_items[:5]):
        add_text_op(ops, x, y + i * line_step, "• " + item["text"], "mono", "black")
        if item["priority"]:
            add_text_op(ops, x + 180, y + i * line_step, "PRIORITY", "mono", "red")

    return {"page_type": "main", "ops": ops}


def resolve_y_range(points, axis_mode, manual_min, manual_max):
    if axis_mode == "manual":
        y_min = float(manual_min)
        y_max = float(manual_max)
        if abs(y_max - y_min) < 1e-9:
            y_max = y_min + 1.0
        if y_max < y_min:
            y_min, y_max = y_max, y_min
        return y_min, y_max

    ys = [p["y"] for p in points] if points else [0.0, 1.0]
    y_min = min(ys)
    y_max = max(ys)
    if abs(y_max - y_min) < 1e-9:
        y_max = y_min + 1.0
    return y_min, y_max


def add_y_ticks(ops, axis_x, plot_top, plot_bottom, y_min, y_max, label_x):
    plot_h = plot_bottom - plot_top
    for frac in [0.0, 0.5, 1.0]:
        py = plot_bottom - int(plot_h * frac)
        value = y_min + (y_max - y_min) * frac
        add_line_op(ops, axis_x - 4, py, axis_x, py, "black")
        tick = f"{value:.1f}"[:6]
        tx = label_x - max(0, (len(tick) - 1) * 6)
        add_text_op(ops, tx, py + 4, tick, "mono", "black")


def map_points_to_coords(points, left, right, top, bottom, y_min, y_max, shared_span=None):
    plot_w = right - left
    plot_h = bottom - top
    if shared_span is None:
        shared_span = max(1, len(points) - 1)
    coords = []
    for i, p in enumerate(points):
        frac_x = 0.0 if shared_span <= 0 else (i / float(shared_span))
        px = int(round(left + frac_x * plot_w))
        py = int(round(bottom - ((p["y"] - y_min) / (y_max - y_min)) * plot_h))
        coords.append((px, py))
    return coords


def draw_series(ops, points, coords, show_avg):
    for i in range(len(coords) - 1):
        add_line_op(ops, coords[i][0], coords[i][1], coords[i + 1][0], coords[i + 1][1], "black")
    for x, y in coords:
        add_cross_op(ops, x, y, "black")
    if show_avg and len(points) >= 3:
        avg_coords = []
        for i in range(2, len(points)):
            avg_y = (points[i]["y"] + points[i - 1]["y"] + points[i - 2]["y"]) / 3.0
            px = coords[i][0]
            avg_coords.append((px, avg_y))
        y_min = min(p["y"] for p in points)
        y_max = max(p["y"] for p in points)
        if abs(y_max - y_min) < 1e-9:
            y_max = y_min + 1.0
        # caller handles rescaling instead in dual/single helpers


def render_single_graph(ops, points, y_label, show_avg, axis_mode, manual_min, manual_max):
    axis_x, right = 86, 770
    top, bottom = 28, 430
    label_x = 44
    plot_left = axis_x + 10
    plot_right = right
    plot_top = top + 18
    plot_bottom = bottom - 24

    add_stacked_label(ops, 14, top + 56, y_label, step=12, font="mono", color="black")
    add_text_op(ops, plot_left + 6, top + 6, y_label[:16], "bold", "black")
    add_line_op(ops, axis_x, plot_bottom, plot_right, plot_bottom, "black")
    add_line_op(ops, axis_x, plot_top, axis_x, plot_bottom, "black")

    if not points:
        add_text_op(ops, 280, 220, "No graph data", "bold", "black")
        return

    y_min, y_max = resolve_y_range(points, axis_mode, manual_min, manual_max)
    add_y_ticks(ops, axis_x, plot_top, plot_bottom, y_min, y_max, label_x)
    shared_span = max(1, len(points) - 1)
    coords = map_points_to_coords(points, plot_left, plot_right, plot_top, plot_bottom, y_min, y_max, shared_span)

    for i in range(len(coords) - 1):
        add_line_op(ops, coords[i][0], coords[i][1], coords[i + 1][0], coords[i + 1][1], "black")
    for x, y in coords:
        add_cross_op(ops, x, y, "black")
    if show_avg and len(points) >= 3:
        avg_pts = []
        for i in range(2, len(points)):
            avg_y = (points[i]["y"] + points[i - 1]["y"] + points[i - 2]["y"]) / 3.0
            px = coords[i][0]
            py = int(round(plot_bottom - ((avg_y - y_min) / (y_max - y_min)) * (plot_bottom - plot_top)))
            avg_pts.append((px, py))
        for i in range(len(avg_pts) - 1):
            add_line_op(ops, avg_pts[i][0], avg_pts[i][1], avg_pts[i + 1][0], avg_pts[i + 1][1], "red")

    label_stride = max(1, math.ceil(len(points) / 6))
    for i, p in enumerate(points):
        if i % label_stride == 0 or i == len(points) - 1:
            px = coords[i][0]
            add_line_op(ops, px, plot_bottom, px, plot_bottom + 4, "black")
            tick = p["x_label"][:8]
            tx = px - int(len(tick) * 3)
            add_text_op(ops, tx, plot_bottom + 16, tick, "mono", "black")


def render_dual_graph(ops, points1, label1, avg1, axis_mode1, manual_min1, manual_max1,
                      points2, label2, avg2, axis_mode2, manual_min2, manual_max2):
    axis_x, right = 86, 770
    label_x = 44
    plot_left = axis_x + 10
    plot_right = right

    top1, bottom1 = 20, 208
    top2, bottom2 = 236, 430
    plot_top1, plot_bottom1 = top1 + 18, bottom1 - 8
    plot_top2, plot_bottom2 = top2 + 18, bottom2 - 24

    shared_span = max(len(points1), len(points2), 1) - 1
    if shared_span < 1:
        shared_span = 1

    def render_box(points, top, bottom, plot_top, plot_bottom, y_label, show_avg, show_x_labels, axis_mode, manual_min, manual_max):
        add_stacked_label(ops, 14, top + 30, y_label, step=11, font="mono", color="black")
        add_text_op(ops, plot_left + 6, top + 6, y_label[:16], "bold", "black")
        add_line_op(ops, axis_x, plot_bottom, plot_right, plot_bottom, "black")
        add_line_op(ops, axis_x, plot_top, axis_x, plot_bottom, "black")
        if not points:
            add_text_op(ops, 300, top + 80, "No data", "mono", "black")
            return
        y_min, y_max = resolve_y_range(points, axis_mode, manual_min, manual_max)
        add_y_ticks(ops, axis_x, plot_top, plot_bottom, y_min, y_max, label_x)
        coords = map_points_to_coords(points, plot_left, plot_right, plot_top, plot_bottom, y_min, y_max, shared_span)
        for i in range(len(coords) - 1):
            add_line_op(ops, coords[i][0], coords[i][1], coords[i + 1][0], coords[i + 1][1], "black")
        for x, y in coords:
            add_cross_op(ops, x, y, "black")
        if show_avg and len(points) >= 3:
            avg_pts = []
            for i in range(2, len(points)):
                avg_y = (points[i]["y"] + points[i - 1]["y"] + points[i - 2]["y"]) / 3.0
                px = coords[i][0]
                py = int(round(plot_bottom - ((avg_y - y_min) / (y_max - y_min)) * (plot_bottom - plot_top)))
                avg_pts.append((px, py))
            for i in range(len(avg_pts) - 1):
                add_line_op(ops, avg_pts[i][0], avg_pts[i][1], avg_pts[i + 1][0], avg_pts[i + 1][1], "red")
        if points:
            label_stride = max(1, math.ceil(max(len(points1), len(points2), 1) / 6))
            for i in range(len(points)):
                px = coords[i][0]
                add_line_op(ops, px, plot_bottom, px, plot_bottom + 4, "black")
                if show_x_labels and (i % label_stride == 0 or i == len(points) - 1):
                    tick = points[i]["x_label"][:8]
                    tx = px - int(len(tick) * 3)
                    add_text_op(ops, tx, plot_bottom + 14, tick, "mono", "black")

    render_box(points1, top1, bottom1, plot_top1, plot_bottom1, label1, avg1, False, axis_mode1, manual_min1, manual_max1)
    render_box(points2, top2, bottom2, plot_top2, plot_bottom2, label2, avg2, True, axis_mode2, manual_min2, manual_max2)


def build_graph_render_job():
    try:
        graph_mode = int(get_setting("graph_mode", "1"))
    except Exception:
        graph_mode = 1
    if graph_mode not in (1, 2):
        graph_mode = 1
    graph1_label = get_setting("graph1_label", "Graph 1")
    graph2_label = get_setting("graph2_label", "Graph 2")
    graph1_avg = get_setting("graph1_avg", "0") == "1"
    graph2_avg = get_setting("graph2_avg", "0") == "1"
    graph1_axis_mode = get_setting("graph1_axis_mode", "auto")
    graph2_axis_mode = get_setting("graph2_axis_mode", "auto")
    graph1_y_min = safe_float_setting("graph1_y_min", 0.0)
    graph1_y_max = safe_float_setting("graph1_y_max", 1.0)
    graph2_y_min = safe_float_setting("graph2_y_min", 0.0)
    graph2_y_max = safe_float_setting("graph2_y_max", 1.0)
    graph1_points = parse_graph_csv(get_setting("graph1_csv", ""))
    graph2_points = parse_graph_csv(get_setting("graph2_csv", ""))
    ops = [{"op": "clear", "color": "white"}]
    if graph_mode == 2:
        render_dual_graph(ops, graph1_points, graph1_label, graph1_avg, graph1_axis_mode, graph1_y_min, graph1_y_max,
                          graph2_points, graph2_label, graph2_avg, graph2_axis_mode, graph2_y_min, graph2_y_max)
    else:
        render_single_graph(ops, graph1_points, graph1_label, graph1_avg, graph1_axis_mode, graph1_y_min, graph1_y_max)
    return {"page_type": "graph", "ops": ops}


def store_render_job(payload_dict):
    conn = db()
    conn.execute("UPDATE render_jobs SET active = 0")
    conn.execute(
        "INSERT INTO render_jobs (page_type, payload, created, active) VALUES (?, ?, ?, 1)",
        (payload_dict.get("page_type", "main"), json.dumps(payload_dict, separators=(",", ":")), int(time.time())),
    )
    conn.commit()
    conn.close()


def rebuild_current_render_job():
    payload = build_graph_render_job() if get_setting("current_page_type", "main") == "graph" else build_main_render_job()
    store_render_job(payload)
    bump_page_revision()
    mark_refresh_needed()


def init_db():
    conn = db()
    conn.execute("CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT NOT NULL, location TEXT NOT NULL, urgent INTEGER NOT NULL DEFAULT 0, created INTEGER NOT NULL, deadline INTEGER NOT NULL, duration_ms INTEGER NOT NULL, reissue_count INTEGER NOT NULL DEFAULT 0, active INTEGER NOT NULL DEFAULT 1)")
    conn.execute("CREATE TABLE IF NOT EXISTS ideas (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT NOT NULL, bold INTEGER NOT NULL DEFAULT 0, active INTEGER NOT NULL DEFAULT 1)")
    conn.execute("CREATE TABLE IF NOT EXISTS partner_items (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0, created INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1)")
    conn.execute("CREATE TABLE IF NOT EXISTS schedule_items (id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT NOT NULL, from_minutes INTEGER NOT NULL, to_minutes INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1)")
    conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("CREATE TABLE IF NOT EXISTS render_jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, page_type TEXT NOT NULL, payload TEXT NOT NULL, created INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1)")
    defaults = {
        "tasks_version": "1", "ideas_version": "1", "partner_version": "1", "schedule_version": "1", "mode_version": "1",
        "refresh_requested": "0", "force_ota": "0", "last_updated": str(int(time.time())), "right_mode": "ideas",
        "current_page_type": "main", "page_revision": "1", "last_acked_job_id": "0", "graph_mode": "1",
        "graph1_label": "Graph 1", "graph2_label": "Graph 2", "graph1_avg": "0", "graph2_avg": "0", "graph1_csv": "", "graph2_csv": "",
        "graph1_axis_mode": "auto", "graph2_axis_mode": "auto", "graph1_y_min": "0", "graph1_y_max": "1", "graph2_y_min": "0", "graph2_y_max": "1"
    }
    for k, v in defaults.items():
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))
    conn.commit()
    idea_cols = [r["name"] for r in conn.execute("PRAGMA table_info(ideas)").fetchall()]
    if "bold" not in idea_cols:
        conn.execute("ALTER TABLE ideas ADD COLUMN bold INTEGER NOT NULL DEFAULT 0")
        conn.commit()
    existing_job = conn.execute("SELECT id FROM render_jobs WHERE active = 1 LIMIT 1").fetchone()
    conn.close()
    if existing_job is None:
        rebuild_current_render_job()


@app.route("/")
def home():
    token = request.args.get("token", "")
    require_token(token)
    conn = db()
    tasks = conn.execute("SELECT * FROM tasks WHERE active = 1 ORDER BY deadline ASC LIMIT 6").fetchall()
    ideas = conn.execute("SELECT * FROM ideas WHERE active = 1 ORDER BY id ASC LIMIT 20").fetchall()
    partner_items = conn.execute("SELECT * FROM partner_items WHERE active = 1 ORDER BY created ASC LIMIT 12").fetchall()
    schedule_rows = conn.execute("SELECT * FROM schedule_items WHERE active = 1 ORDER BY from_minutes ASC LIMIT 4").fetchall()
    conn.close()
    rendered_tasks = [{"id": t["id"], "text": t["text"], "location": t["location"], "urgent": bool(t["urgent"]), "deadline_str": deadline_str(t["deadline"]), "reissue_count": t["reissue_count"]} for t in tasks]
    rendered_ideas = [{"id": i["id"], "text": i["text"], "bold": bool(i["bold"])} for i in ideas]
    rendered_partner = [{"id": p["id"], "text": p["text"], "priority": bool(p["priority"])} for p in partner_items]
    rendered_schedule = [{"id": r["id"], "label": r["label"], "from_time": minutes_to_time_str(r["from_minutes"]), "to_time": minutes_to_time_str(r["to_minutes"])} for r in schedule_rows]
    return render_template_string(PAGE, token=token, tasks=rendered_tasks, ideas=rendered_ideas, partner_items=rendered_partner, schedule_rows=rendered_schedule, right_mode=get_setting("right_mode", "ideas"), current_page_type=get_setting("current_page_type", "main"), graph_mode=int(get_setting("graph_mode", "1")), graph1_label=get_setting("graph1_label", "Graph 1"), graph2_label=get_setting("graph2_label", "Graph 2"), graph1_avg=get_setting("graph1_avg", "0") == "1", graph2_avg=get_setting("graph2_avg", "0") == "1", graph1_csv=get_setting("graph1_csv", ""), graph2_csv=get_setting("graph2_csv", ""), graph1_axis_mode=get_setting("graph1_axis_mode", "auto"), graph2_axis_mode=get_setting("graph2_axis_mode", "auto"), graph1_y_min=get_setting("graph1_y_min", "0"), graph1_y_max=get_setting("graph1_y_max", "1"), graph2_y_min=get_setting("graph2_y_min", "0"), graph2_y_max=get_setting("graph2_y_max", "1"))


@app.route("/partner")
def partner_page():
    token = request.args.get("token", "")
    require_partner_token(token)
    return render_template_string(PARTNER_PAGE, token=token)


@app.route("/add", methods=["POST"])
def add_task():
    token = request.form.get("token", "")
    require_token(token)
    dt = datetime.strptime(request.form["deadline_local"].strip(), "%Y-%m-%dT%H:%M")
    deadline = int(dt.timestamp())
    now = int(time.time())
    duration_ms = max(0, (deadline - now)) * 1000
    conn = db()
    conn.execute("INSERT INTO tasks (text, location, urgent, created, deadline, duration_ms, reissue_count, active) VALUES (?, ?, ?, ?, ?, ?, 0, 1)", (request.form["text"].strip(), request.form["location"].strip(), 1 if request.form.get("urgent") else 0, now, deadline, duration_ms))
    conn.commit(); conn.close()
    bump_version("tasks_version"); rebuild_current_render_job()
    return redirect(f"/?token={token}")


@app.route("/delete")
def delete_task():
    token = request.args.get("token", "")
    require_token(token)
    conn = db(); conn.execute("UPDATE tasks SET active = 0 WHERE id = ?", (int(request.args["id"]),)); conn.commit(); conn.close()
    bump_version("tasks_version"); rebuild_current_render_job()
    return redirect(f"/?token={token}")


@app.route("/reissue")
def reissue_task():
    token = request.args.get("token", "")
    require_token(token)
    task_id = int(request.args["id"])
    now = int(time.time())
    conn = db()
    row = conn.execute("SELECT duration_ms, reissue_count FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if row is None:
        conn.close(); abort(404)
    new_deadline = now + (int(row["duration_ms"]) // 1000)
    conn.execute("UPDATE tasks SET created = ?, deadline = ?, reissue_count = ? WHERE id = ?", (now, new_deadline, int(row["reissue_count"]) + 1, task_id))
    conn.commit(); conn.close()
    bump_version("tasks_version"); rebuild_current_render_job()
    return redirect(f"/?token={token}")


@app.route("/add_idea", methods=["POST"])
def add_idea():
    token = request.form.get("token", "")
    require_token(token)
    conn = db(); conn.execute("INSERT INTO ideas (text, bold, active) VALUES (?, ?, 1)", (request.form["text"].strip(), 1 if request.form.get("bold") else 0)); conn.commit(); conn.close()
    bump_version("ideas_version"); rebuild_current_render_job()
    return redirect(f"/?token={token}")


@app.route("/remove_idea")
def remove_idea():
    token = request.args.get("token", "")
    require_token(token)
    conn = db(); conn.execute("UPDATE ideas SET active = 0 WHERE id = ?", (int(request.args["id"]),)); conn.commit(); conn.close()
    bump_version("ideas_version"); rebuild_current_render_job()
    return redirect(f"/?token={token}")


@app.route("/partner_add", methods=["POST"])
def partner_add():
    token = request.form.get("token", "")
    require_partner_token(token)
    conn = db(); conn.execute("INSERT INTO partner_items (text, priority, created, active) VALUES (?, ?, ?, 1)", (request.form["text"].strip(), 1 if request.form.get("priority") else 0, int(time.time()))); conn.commit(); conn.close()
    bump_version("partner_version"); rebuild_current_render_job()
    return redirect(f"/partner?token={token}")


@app.route("/remove_partner")
def remove_partner():
    token = request.args.get("token", "")
    require_token(token)
    conn = db(); conn.execute("UPDATE partner_items SET active = 0 WHERE id = ?", (int(request.args["id"]),)); conn.commit(); conn.close()
    bump_version("partner_version"); rebuild_current_render_job()
    return redirect(f"/?token={token}")


@app.route("/set_mode", methods=["POST"])
def set_mode():
    token = request.form.get("token", "")
    require_token(token)
    mode = request.form.get("mode", "ideas")
    if mode not in ("ideas", "schedule"): mode = "ideas"
    set_setting("right_mode", mode)
    bump_version("mode_version"); rebuild_current_render_job()
    return redirect(f"/?token={token}")


@app.route("/add_schedule", methods=["POST"])
def add_schedule():
    token = request.form.get("token", "")
    require_token(token)
    from_minutes = parse_time_to_minutes(request.form["from_time"].strip())
    to_minutes = parse_time_to_minutes(request.form["to_time"].strip())
    if to_minutes < from_minutes:
        return redirect(f"/?token={token}")
    conn = db(); conn.execute("INSERT INTO schedule_items (label, from_minutes, to_minutes, active) VALUES (?, ?, ?, 1)", (request.form["label"].strip(), from_minutes, to_minutes)); conn.commit(); conn.close()
    bump_version("schedule_version"); rebuild_current_render_job()
    return redirect(f"/?token={token}")


@app.route("/remove_schedule")
def remove_schedule():
    token = request.args.get("token", "")
    require_token(token)
    conn = db(); conn.execute("UPDATE schedule_items SET active = 0 WHERE id = ?", (int(request.args["id"]),)); conn.commit(); conn.close()
    bump_version("schedule_version"); rebuild_current_render_job()
    return redirect(f"/?token={token}")


@app.route("/set_page", methods=["POST"])
def set_page():
    token = request.form.get("token", "")
    require_token(token)
    page_type = request.form.get("page_type", "main")
    if page_type not in ("main", "graph"): page_type = "main"
    set_setting("current_page_type", page_type)
    rebuild_current_render_job()
    return redirect(f"/?token={token}")


@app.route("/graph_settings", methods=["POST"])
def graph_settings():
    token = request.form.get("token", "")
    require_token(token)
    set_setting("graph_mode", request.form.get("graph_mode", "1") if request.form.get("graph_mode", "1") in ("1", "2") else "1")
    set_setting("graph1_label", request.form.get("graph1_label", "Graph 1").strip()[:18])
    set_setting("graph2_label", request.form.get("graph2_label", "Graph 2").strip()[:18])
    set_setting("graph1_avg", "1" if request.form.get("graph1_avg") else "0")
    set_setting("graph2_avg", "1" if request.form.get("graph2_avg") else "0")
    set_setting("graph1_axis_mode", request.form.get("graph1_axis_mode", "auto") if request.form.get("graph1_axis_mode", "auto") in ("auto", "manual") else "auto")
    set_setting("graph2_axis_mode", request.form.get("graph2_axis_mode", "auto") if request.form.get("graph2_axis_mode", "auto") in ("auto", "manual") else "auto")
    set_setting("graph1_y_min", request.form.get("graph1_y_min", "0").strip())
    set_setting("graph1_y_max", request.form.get("graph1_y_max", "1").strip())
    set_setting("graph2_y_min", request.form.get("graph2_y_min", "0").strip())
    set_setting("graph2_y_max", request.form.get("graph2_y_max", "1").strip())
    set_setting("graph1_csv", request.form.get("graph1_csv", "").strip())
    set_setting("graph2_csv", request.form.get("graph2_csv", "").strip())
    rebuild_current_render_job()
    return redirect(f"/?token={token}")


@app.route("/refresh_now", methods=["POST"])
def refresh_now():
    token = request.form.get("token", "")
    require_token(token)
    rebuild_current_render_job()
    return redirect(f"/?token={token}")


@app.route("/force_update", methods=["POST"])
def force_update():
    token = request.form.get("token", "")
    require_token(token)
    set_setting("force_ota", "1")
    return redirect(f"/?token={token}")


@app.route("/api/meta")
def api_meta():
    token = request.args.get("token", "")
    require_token(token)

    conn = db()
    row = conn.execute("SELECT id, page_type FROM render_jobs WHERE active = 1 ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()

    job_id = int(row["id"]) if row else 0
    page_type = row["page_type"] if row else get_setting("current_page_type", "main")
    force = int(get_setting("force_ota", "0"))

    response = {
        "page_revision": int(get_setting("page_revision", "1")),
        "page_type": page_type,
        "job_ready": 1 if job_id > 0 else 0,
        "job_id": job_id,
        "refresh_requested": int(get_setting("refresh_requested", "0")),
        "force_ota": force,
        "firmware_version": FIRMWARE_VERSION,
        "last_updated": int(get_setting("last_updated", str(int(time.time())))),
    }

    if force == 1:
        set_setting("force_ota", "0")

    return jsonify(response)


@app.route("/api/render_job")
def api_render_job():
    token = request.args.get("token", "")
    require_token(token)
    requested_job_id = int(request.args.get("job_id", "0"))
    conn = db()
    if requested_job_id > 0:
        row = conn.execute("SELECT id, page_type, payload FROM render_jobs WHERE id = ? LIMIT 1", (requested_job_id,)).fetchone()
    else:
        row = conn.execute("SELECT id, page_type, payload FROM render_jobs WHERE active = 1 ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    if row is None:
        return jsonify({"ok": False, "error": "no_job"}), 404
    payload = json.loads(row["payload"])
    return jsonify({"ok": True, "job_id": int(row["id"]), "page_type": row["page_type"], "payload": payload})


@app.route("/api/ack_job", methods=["POST"])
def api_ack_job():
    token = request.args.get("token", "")
    require_token(token)
    set_setting("last_acked_job_id", str(int(request.args.get("job_id", "0"))))
    set_setting("refresh_requested", "0")
    return jsonify({"ok": True})


@app.route("/api/ack_ota", methods=["POST"])
def api_ack_ota():
    token = request.args.get("token", "")
    require_token(token)
    set_setting("force_ota", "0")
    return jsonify({"ok": True})


@app.route("/api/firmware_meta")
def api_firmware_meta():
    token = request.args.get("token", "")
    require_token(token)
    base = request.host_url.rstrip("/")
    return jsonify({"version": FIRMWARE_VERSION, "url": base + "/firmware/device_b.bin"})


@app.route("/firmware/device_b.bin")
def firmware_bin():
    if not os.path.exists(FIRMWARE_PATH):
        abort(404)
    return send_file(FIRMWARE_PATH, mimetype="application/octet-stream", as_attachment=False)


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000)
else:
    init_db()
