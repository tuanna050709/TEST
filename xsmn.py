#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import gspread
import json
import sys
import re
import time

# -------- CONFIG --------
URL = "https://xosodaiphat.com/xsmn-200-ngay.html"
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
WORKSHEET_NAME = "MN"
HTML_FILE = "xsmn.html"

# -------- Google Sheets auth --------
try:
    creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    gc = gspread.service_account_from_dict(creds_json)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="3000", cols="20")
except KeyError:
    print("❌ Không tìm thấy biến môi trường 'GOOGLE_CREDENTIALS'.")
    sys.exit(1)
except Exception as e:
    print(f"❌ Lỗi khi kết nối Google Sheets: {e}")
    sys.exit(1)


# -------- Fetch & Parse --------
def fetch_data():
    """Cào dữ liệu XSMN và trả về DataFrame."""
    if os.path.exists(HTML_FILE):
        print(f"📂 Reading local HTML: {HTML_FILE} ...")
        with open(HTML_FILE, "r", encoding="utf-8") as f:
            html = f.read()
    else:
        print(f"🌍 Fetching online: {URL} ...")
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(URL, headers=headers, timeout=30)
        resp.raise_for_status()
        html = resp.text

    soup = BeautifulSoup(html, "html.parser")
    blocks = soup.select("div.block")
    if not blocks:
        raise ValueError("❌ Không tìm thấy khối dữ liệu XSMN (div.block).")

    records = []

    for block in blocks:
        # Lấy ngày
        date_tag = block.find("h2", class_="class-title-list-link")
        if not date_tag:
            continue
        date_match = re.search(r"\d{2}/\d{2}/\d{4}", date_tag.get_text(strip=True))
        if not date_match:
            continue
        ngay = datetime.strptime(date_match.group(), "%d/%m/%Y").strftime("%Y-%m-%d")

        # Lấy text toàn khối
        text_block = block.get_text(separator="\n", strip=True)

        # -------- Giải 8 --------
        g8_line = re.search(r"G\.8\s*([\d\s]+)", text_block)
        g8_list = g8_line.group(1).split() if g8_line else []
        g8_str = " ".join(g8_list[:10])  # đề phòng có 4 đài

        # -------- Giải ĐB --------
        gdb_line = re.search(r"G\.ĐB\s*([\d\s]+)", text_block)
        gdb_list = gdb_line.group(1).split() if gdb_line else []
        gdb_str = " ".join(gdb_list[:10])

        if g8_str or gdb_str:
            records.append({
                "Ngày": ngay,
                "Tỉnh": "MN",
                "Giải 8": g8_str,
                "Giải ĐB": gdb_str
            })

    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["Ngày"], keep="first")
    df = df.sort_values("Ngày", ascending=False).reset_index(drop=True)
    return df


# -------- Main logic --------
def main():
    print("🚀 Bắt đầu cào dữ liệu XSMN...")
    df = fetch_data()
    if df.empty:
        print("❌ Không lấy được dữ liệu nào.")
        return

    print(f"✅ Đã cào được {len(df)} dòng dữ liệu.")
    print(df.head(5).to_string())

    # -------- XÓA TOÀN BỘ DỮ LIỆU CŨ --------
    try:
        ws.clear()
        print("🧹 Đã xoá toàn bộ dữ liệu cũ trong sheet.")
    except Exception as e:
        print(f"⚠️ Lỗi khi xoá dữ liệu: {e}")

    # -------- THÊM HEADER --------
    ws.append_row(list(df.columns))
    print("📝 Đã thêm header cho sheet.")

    # -------- GHI DỮ LIỆU MỚI --------
    rows_to_add = [r.to_list() for _, r in df.iterrows()]  # ✅ FIXED: không dùng .values()
    for i in range(0, len(rows_to_add), 50):
        batch = rows_to_add[i:i + 50]
        ws.append_rows(batch)
        print(f"✅ Đã ghi {len(batch)} dòng...")
        time.sleep(2)

    print(f"🎯 Hoàn tất: {len(df)} dòng được ghi mới, ngày mới nhất ở trên cùng.")


if __name__ == "__main__":
    main()