#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, re, requests, pandas as pd, sys
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time 
import gspread 
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import WorksheetNotFound, APIError

# -------- CONFIG ĐÃ SỬA --------
URL_DATE_TEMPLATE = "https://www.xosominhngoc.com/ket-qua-xo-so/mien-bac/{dd}-{mm}-{yyyy}.html"
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "MB")

# Mục tiêu lấy 360 kỳ mở thưởng
MAX_DAYS_TO_FETCH = 500 
# Đảm bảo vòng lặp duyệt đủ ngày để lấy 360 kỳ mở thưởng (~500 ngày dương lịch)
MAX_DATE_OFFSET = 500 

HEADERS = {
    'User-Agent': 'Mozilla/50 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image:apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Referer': 'https://www.google.com/', 
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0' 
}
# Cấu trúc cột để đảm bảo thứ tự
COLS_A_J = ["Ngày", "Giải ĐB", "Giải 1", "Giải 2", "Giải 3", "Giải 4", "Giải 5", "Giải 6", "Giải 7", "Tất cả"]

# -------- Google Sheets auth & Kết nối (Giữ nguyên) --------
try:
    creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(WORKSHEET_NAME) 
    except WorksheetNotFound:
        print(f"⚠️ Worksheet '{WORKSHEET_NAME}' không tìm thấy. Đang tạo Worksheet mới.")
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="3000", cols="20")
except KeyError:
    print("❌ Lỗi: Thiếu biến môi trường GOOGLE_CREDENTIALS.")
    sys.exit(1)
except APIError as e:
    print(f"❌ Lỗi API Google Sheets (Quyền truy cập?): {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Lỗi kết nối Google Sheets: {e}")
    sys.exit(1)
print("✅ Đã kết nối thành công tới Google Sheets API.")


# -------- Helpers (Giữ nguyên logic trích xuất) --------
CLASS_TO_COL = {
    "giaidb": "Giải ĐB", "giai1": "Giải 1", "giai2": "Giải 2", "giai3": "Giải 3", 
    "giai4": "Giải 4", "giai5": "Giải 5", "giai6": "Giải 6", "giai7": "Giải 7",
}

def clean_and_extract_numbers(td_element):
    td_content = str(td_element)
    clean_text = re.sub(r'<[^>]+>', ' ', td_content).strip()
    nums = re.findall(r"\b\d{2,}\b", clean_text)
    if nums:
        unique_nums = []
        [unique_nums.append(n) for n in nums if n not in unique_nums]
        return " ".join(unique_nums)
    return ""

def find_date_and_clean_rec(tbl, expected_date_str):
    date_str = None
    ngay_element = tbl.find("td", class_="ngay")
    if ngay_element:
        text = ngay_element.get_text(" ", strip=True)
        m = re.search(r"(\d{2}/\d{2}/\d{4})", text)
        if m: date_str = m.group(1)
            
    if not date_str: date_str = expected_date_str
        
    try:
        date_formatted = datetime.strptime(date_str, "%d/%m/%Y").strftime("%d/%m/%Y")
    except:
        return None, None

    giải_dict = {f"Giải {i}": "" for i in ["ĐB", 1, 2, 3, 4, 5, 6, 7]}
    rec = {"Ngày": date_formatted, **giải_dict}
    
    for class_name, col_name in CLASS_TO_COL.items():
        if class_name == 'giaidb':
            tr_db = tbl.find("tr", class_=re.compile(r'giaidb|db'))
            if tr_db:
                td_db_value = tr_db.find_all('td')
                if len(td_db_value) > 1:
                    rec[col_name] = clean_and_extract_numbers(td_db_value[1])
            if not rec[col_name]:
                 td_element = tbl.find("td", class_='giaidb')
                 if td_element:
                     rec[col_name] = clean_and_extract_numbers(td_element)
        else:
            td_element = tbl.find("td", class_=class_name)