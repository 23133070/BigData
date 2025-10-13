import pandas as pd
import re
import os
import unicodedata

# === Cấu hình đường dẫn ===
BASE_DIR = "D:\\bigdata"
input_file = os.path.join(BASE_DIR, "car_data_clean.csv")   # file gốc sau bước Clean
output_csv = os.path.join(BASE_DIR,  "car_data_clean_standardized.csv")
output_parquet = os.path.join(BASE_DIR, "car_data_normalized.parquet")

print("👉 Đang đọc file tại:", input_file)

# --- Hàm bỏ dấu + tạo snake_case an toàn ---
def vi_to_ascii_snake(s: str) -> str:
    # Bỏ dấu tiếng Việt
    s_norm = unicodedata.normalize("NFD", s)
    s_ascii = "".join(ch for ch in s_norm if unicodedata.category(ch) != "Mn")
    # Về chữ thường + thay ký tự không phải chữ/số thành _
    s_ascii = re.sub(r"[^0-9a-zA-Z]+", "_", s_ascii).strip("_").lower()
    return s_ascii

# --- Ánh xạ tên cột Việt -> Anh (ưu tiên) ---
VI_TO_EN_MAP = {
    "giá": "price",
    "gia": "price",
    "năm_sản_xuất": "year",
    "nam_san_xuat": "year",
    "số_km_đã_đi": "mileage",
    "so_km_da_di": "mileage",
    "tên_xe": "car_name",
    "ten_xe": "car_name",
    "kiểu_dáng": "body_style",
    "kieu_dang": "body_style",
    "tình_trạng": "condition",
    "tinh_trang": "condition",
    "xuất_xứ": "origin",
    "xuat_xu": "origin",
    "tỉnh_thành": "province",
    "tinh_thanh": "province",
    "hộp_số": "transmission",
    "hop_so": "transmission",
    "nhiên_liệu": "fuel",
    "nhien_lieu": "fuel",
}

# --- Bước 1: Đọc file CSV gốc ---
df = pd.read_csv(input_file)

# --- Bước 1.1: Chuẩn hóa tên cột sang tiếng Anh ---
# Chuẩn hóa khóa của map (loại bỏ dấu/đưa về snake) để khớp bền vững
normalized_map = {vi_to_ascii_snake(k): v for k, v in VI_TO_EN_MAP.items()}

new_columns = {}
for col in df.columns:
    key = vi_to_ascii_snake(str(col))
    if key in normalized_map:
        new_columns[col] = normalized_map[key]
    else:
        # Fallback: dùng bản snake_case bỏ dấu như "mau_son", "dang_ky", ...
        new_columns[col] = key

df = df.rename(columns=new_columns)

# ---- Tên cột tiếng Anh sau chuẩn hóa ----
# Sử dụng xuyên suốt các bước dưới đây
COL_PRICE = "price"
COL_YEAR = "year"
COL_MILEAGE = "mileage"
COL_ORIGIN = "origin"

# --- Bước 2: Xử lý giá trị rỗng ---
required_cols = [COL_PRICE, COL_YEAR]
df = df.dropna(subset=[c for c in required_cols if c in df.columns])

for c in required_cols:
    if c in df.columns:
        df = df[df[c].astype(str).str.strip() != ""]

# --- Bước 3: Làm sạch cột số ---
if COL_PRICE in df.columns:
    df[COL_PRICE] = df[COL_PRICE].astype(str).str.replace(r"\D", "", regex=True)
    df[COL_PRICE] = pd.to_numeric(df[COL_PRICE], errors="coerce")

if COL_YEAR in df.columns:
    df[COL_YEAR] = df[COL_YEAR].astype(str).str.extract(r"(\d{4})")
    df[COL_YEAR] = pd.to_numeric(df[COL_YEAR], errors="coerce")

df = df.dropna(subset=[c for c in required_cols if c in df.columns])

# --- Bước 4: Lọc dữ liệu không hợp lệ ---
if COL_YEAR in df.columns:
    df = df[df[COL_YEAR].between(1950, 2025)]
if COL_PRICE in df.columns:
    df = df[(df[COL_PRICE] > 1_000_000) & (df[COL_PRICE] < 10**12)]

# --- Bước 5: Chuẩn hóa mileage (nếu có) ---
if COL_MILEAGE in df.columns:
    df[COL_MILEAGE] = df[COL_MILEAGE].astype(str).apply(lambda x: re.sub(r'[^0-9]', '', x))
    df[COL_MILEAGE] = pd.to_numeric(df[COL_MILEAGE], errors="coerce")

# --- Bước 6: Chuẩn hóa text ---
text_cols = [
    "car_name", "body_style", "condition",
    COL_ORIGIN, "province", "transmission", "fuel"
]
for col in text_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.title()

# --- Bước 7: Chuẩn hóa riêng cho origin ---
if COL_ORIGIN in df.columns:
    df[COL_ORIGIN] = df[COL_ORIGIN].str.lower().str.strip()
    df[COL_ORIGIN] = df[COL_ORIGIN].replace({
        "xuất xứ nhập khẩu": "nhập khẩu",
        "xuat xu nhap khau": "nhập khẩu",
        "xuất xứ trong nước": "trong nước",
        "xuat xu trong nuoc": "trong nước",
        "na": "khác",
        "": "khác"
    })
    # Đưa về tiếng Anh rõ ràng
    df[COL_ORIGIN] = df[COL_ORIGIN].replace({
        "nhập khẩu": "imported",
        "trong nước": "domestic",
        "khác": "other"
    })
    df[COL_ORIGIN] = df[COL_ORIGIN].str.title()

# --- Bước 8: Xuất kết quả ---
os.makedirs(os.path.dirname(output_csv), exist_ok=True)

df.to_csv(output_csv, index=False, encoding="utf-8-sig")
df.to_parquet(output_parquet, index=False)

print("✅ Chuẩn hóa xong (đã đổi tên cột sang tiếng Anh)!")
print("📂 CSV:", output_csv)
print("📂 Parquet:", output_parquet)
