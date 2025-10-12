import pandas as pd
import re
import os

# === Cấu hình đường dẫn ===
BASE_DIR = "D:\\bigdata"
input_file = os.path.join(BASE_DIR, "car_data_clean.csv")   # file gốc sau bước Clean
output_csv = os.path.join(BASE_DIR,  "car_data_clean_standardized.csv")
output_parquet = os.path.join(BASE_DIR, "car_data_normalized.parquet")

print("👉 Đang đọc file tại:", input_file)

# --- Bước 1: Đọc file CSV gốc ---
df = pd.read_csv(input_file)

# --- Bước 2: Xử lý giá trị rỗng ---
df = df.dropna(subset=["giá", "năm_sản_xuất"])
df = df[(df["giá"].astype(str).str.strip() != "")]
df = df[(df["năm_sản_xuất"].astype(str).str.strip() != "")]

# --- Bước 3: Làm sạch cột số ---
df["giá"] = df["giá"].astype(str).str.replace(r"\D", "", regex=True)
df["giá"] = pd.to_numeric(df["giá"], errors="coerce")

df["năm_sản_xuất"] = df["năm_sản_xuất"].astype(str).str.extract(r"(\d{4})")
df["năm_sản_xuất"] = pd.to_numeric(df["năm_sản_xuất"], errors="coerce")

df = df.dropna(subset=["giá", "năm_sản_xuất"])

# --- Bước 4: Lọc dữ liệu không hợp lệ ---
df = df[df["năm_sản_xuất"].between(1950, 2025)]
df = df[(df["giá"] > 1_000_000) & (df["giá"] < 10**12)]

# --- Bước 5: Chuẩn hóa số_km_đã_đi (nếu có) ---
if "số_km_đã_đi" in df.columns:
    df["số_km_đã_đi"] = df["số_km_đã_đi"].astype(str).apply(lambda x: re.sub(r'[^0-9]', '', x))
    df["số_km_đã_đi"] = pd.to_numeric(df["số_km_đã_đi"], errors="coerce")

# --- Bước 6: Chuẩn hóa text ---
text_cols = ['tên_xe', 'kiểu_dáng', 'tình_trạng',
             'xuất_xứ', 'tỉnh_thành', 'hộp_số', 'nhiên_liệu']

for col in text_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.title()

# --- Bước 7: Chuẩn hóa riêng cho xuất_xứ ---
if "xuất_xứ" in df.columns:
    df["xuất_xứ"] = df["xuất_xứ"].str.lower().str.strip()
    df["xuất_xứ"] = df["xuất_xứ"].replace({
        "xuất xứ nhập khẩu": "nhập khẩu",
        "xuất xứ trong nước": "trong nước",
        "na": "khác",
        "": "khác"
    })
    df["xuất_xứ"] = df["xuất_xứ"].str.title()

# --- Bước 8: Xuất kết quả ---
os.makedirs(os.path.dirname(output_csv), exist_ok=True)

df.to_csv(output_csv, index=False, encoding="utf-8-sig")
df.to_parquet(output_parquet, index=False)

print("✅ Chuẩn hóa xong!")
print("📂 CSV:", output_csv)
print("📂 Parquet:", output_parquet)