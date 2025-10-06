#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, re, requests, pandas as pd, sys
from bs4 import BeautifulSoup
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# -------- CONFIG --------
URL = "https://xosodaiphat.com/xsmb-200-ngay.html"
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "MB")

# -------- Google Sheets auth --------
creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)
try:
    ws = sh.worksheet(WORKSHEET_NAME)
except:
    ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="3000", cols="20")

# -------- Helpers --------
def find_date_for_table(tbl):
    for a in tbl.find_all_previous("a", limit=8):
        m = re.search(r"(\d{2}/\d{2}/\d{4})", (a.get("title") or a.get_text(" ", strip=True)))
        if m: return m.group(1)
    for tag in ("h2", "h3"):
        el = tbl.find_previous(tag)
        if el:
            m = re.search(r"(\d{2}/\d{2}/\d{4})", el.get_text(" ", strip=True))
            if m: return m.group(1)
    return None

def numbers_from_tr(tr):
    nums = []
    for sp in tr.find_all("span"):
        nums += re.findall(r"\d+", sp.get_text(" ", strip=True))
    if nums: return nums
    for td in tr.find_all("td")[1:]:
        nums += re.findall(r"\d+", td.get_text(" ", strip=True))
    return nums

def label_to_col(label):
    s = re.sub(r'[\s\.\:]+', ' ', label.lower())
    if re.search(r'đb|đặc|dacbiet|g\.?đb|gdb', s): return "Giải ĐB"
    for i in range(1, 8):
        if re.search(fr'\bgiải\s*{i}\b|\bg\.?{i}\b', s): return f"Giải {i}"
    return None

# -------- Fetch page --------
resp = requests.get(URL, timeout=30)
resp.encoding = "utf-8"
soup = BeautifulSoup(resp.text, "html.parser")
tables = soup.select("table.table-xsmb")
if not tables:
    print("❌ Không tìm thấy bảng kết quả.")
    sys.exit(1)

records = []
for tbl in tables:
    date_raw = find_date_for_table(tbl)
    if not date_raw:
        continue
    try:
        date_str = datetime.strptime(date_raw, "%d/%m/%Y").strftime("%d/%m/%Y")
    except:
        continue

    giải_dict = {f"Giải {i}": "" for i in ["ĐB", 1, 2, 3, 4, 5, 6, 7]}
    rec = {"Ngày": date_str, **giải_dict}

    for tr in tbl.select("tr"):
        tds = tr.find_all("td")
        if not tds: continue
        label = tds[0].get_text(" ", strip=True)
        if "mã" in label.lower() and "đb" in label.lower(): continue
        col = label_to_col(label)
        nums = numbers_from_tr(tr)
        if not nums: continue
        joined = " ".join(nums)
        if col:
            rec[col] = joined
        else:
            for k in [f"Giải {i}" for i in range(1, 8)]:
                if not rec[k]:
                    rec[k] = joined
                    break

    if not rec["Giải ĐB"]:
        sp = tbl.select_one("span.special-prize-lg") or tbl.select_one("span.special-code")
        if sp:
            found = re.findall(r"\d+", sp.get_text())
            if found: rec["Giải ĐB"] = " ".join(found)

    # Xử lý loại bỏ trùng số và tạo cột "Tất cả"
    used = set()
    for col in reversed([f"Giải {i}" for i in ["ĐB", 1, 2, 3, 4, 5, 6, 7]]):
        nums = rec[col].split()
        filtered = [n for n in nums if n not in used]
        rec[col] = " ".join(filtered)
        used.update(filtered)
    rec["Tất cả"] = " ".join([rec[f"Giải {i}"] for i in ["ĐB", 1, 2, 3, 4, 5, 6, 7]]).strip()

    records.append(rec)

# -------- Convert to DataFrame --------
df_all = pd.DataFrame(records)
df_all["__dt"] = pd.to_datetime(df_all["Ngày"], format="%d/%m/%Y", errors="coerce")
df_all = df_all.dropna(subset=["__dt"]).sort_values("__dt", ascending=False).drop(columns="__dt").reset_index(drop=True)

# -------- Write back (prepend only new rows, only columns A–J) --------
cols_A_J = ["Ngày", "Giải ĐB", "Giải 1", "Giải 2", "Giải 3", "Giải 4", "Giải 5", "Giải 6", "Giải 7", "Tất cả"]

# Kiểm tra header (cột A)
header = ws.row_values(1)
if not header:
    # Sheet trống -> ghi header + toàn bộ df_all
    rows = [cols_A_J] + df_all[cols_A_J].astype(str).values.tolist()
    ws.update(f"A1:J{len(rows)}", rows)
    print(f"✅ Sheet trống. Ghi {len(df_all)} ngày vào sheet {WORKSHEET_NAME}.")
else:
    # Lấy danh sách ngày hiện có (cột A), bỏ header
    existing_dates = ws.col_values(1)[1:]  # danh sách string, skip header
    # Tìm những dòng thực sự mới (ngày chưa có)
    new_df = df_all[~df_all["Ngày"].isin(existing_dates)].reset_index(drop=True)

    if new_df.empty:
        print("ℹ️ Không có ngày mới để chèn.")
    else:
        num_new = len(new_df)
        # Chèn num_new hàng trống ngay sau header (row 2) để đẩy tất cả cột (bao gồm K,L) xuống
        blank_rows = [[""] * ws.col_count for _ in range(num_new)]
        ws.insert_rows(blank_rows, row=2)  # chèn hàng rỗng toàn bộ chiều rộng sheet

        # Ghi dữ liệu mới vào A2:J(1+num_new)
        values_to_write = new_df[cols_A_J].astype(str).values.tolist()
        ws.update(f"A2:J{1 + num_new}", values_to_write)

        print(f"✅ Đã chèn {num_new} ngày mới vào sheet {WORKSHEET_NAME}. Tổng rows hiện tại: {ws.row_count}")