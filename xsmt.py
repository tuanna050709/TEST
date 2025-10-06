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

# -------- CONFIG --------
URL = "https://xosodaiphat.com/xsmt-200-ngay.html"
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
WORKSHEET_NAME = "MT"
HTML_FILE = "xsmt.html"

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
    if os.path.exists(HTML_FILE):
        print(f"📂 Reading local HTML: {HTML_FILE} ...")
        with open(HTML_FILE, "r", encoding="utf-8") as f:
            html = f.read()
    else:
        print(f"🌍 Fetching online: {URL} ...")
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            resp = requests.get(URL, headers=headers, timeout=20)
            resp.raise_for_status()
            html = resp.text
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"❌ Lỗi khi tải dữ liệu từ {URL}: {e}")

    soup = BeautifulSoup(html, "html.parser")
    blocks = soup.select("div.block")
    if not blocks:
        raise ValueError("❌ Không tìm thấy khối dữ liệu XSMT (div.block).")

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

        text_block = block.get_text(separator="\n", strip=True)

        # -------- Giải 8 --------
        g8_line = re.search(r"G\.8\s*([\d\s]+)", text_block)
        g8_list = g8_line.group(1).split()[:3] if g8_line else []
        g8_str = " ".join(g8_list)

        # -------- Giải ĐB --------
        gdb_line = re.search(r"G\.ĐB\s*([\d\s]+)", text_block)
        gdb_list = gdb_line.group(1).split()[:3] if gdb_line else []
        gdb_str = " ".join(gdb_list)

        if g8_list or gdb_list:
            records.append(
                {"Ngày": ngay, "Tỉnh": "MT", "Giải 8": g8_str, "Giải ĐB": gdb_str}
            )

    df = pd.DataFrame(records)
    return df


# -------- Main logic --------
def main():
    try:
        df = fetch_data()
        if df.empty:
            print("❌ Không lấy được dữ liệu nào.")
            return

        print(f"✅ Đã cào được {len(df)} dòng dữ liệu.")
        print(df.head(5).to_string())

        # Lấy ngày mới nhất trong dữ liệu vừa cào
        latest_row = df.iloc[0].to_dict()
        ngay_moi = latest_row["Ngày"]

        # Lấy toàn bộ dữ liệu hiện tại trong sheet
        existing = ws.get_all_records()
        existing_dates = [row.get("Ngày") for row in existing]

        if ngay_moi in existing_dates:
            print(f"ℹ️ Ngày {ngay_moi} đã tồn tại trong sheet, bỏ qua.")
            return

        # Nếu sheet đang trống thì viết header trước
        if not existing:
            ws.append_row(list(latest_row.keys()))
            print("📝 Đã tạo header cho sheet.")

        # Chèn dữ liệu mới vào dòng 2 (ngay dưới header)
        ws.insert_row(list(latest_row.values()), 2)
        print(f"✅ Đã chèn thêm dữ liệu ngày {ngay_moi} vào sheet '{WORKSHEET_NAME}'.")

    except Exception as e:
        print(f"❌ Đã xảy ra lỗi nghiêm trọng: {e}")
        raise


if __name__ == "__main__":
    main()
