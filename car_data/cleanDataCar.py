import pandas as pd
import numpy as np

# Đọc dữ liệu
df = pd.read_csv("car_data_crawl.csv")

# Chuẩn hoá tên cột
df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

# Giữ nguyên giá trị gốc của cột giá và số_km_đã_đi
# Chỉ loại bỏ dòng bị thiếu ở 2 cột này
df = df.dropna(subset=["giá", "số_km_đã_đi"])

# Các cột khác nếu thiếu thì gán "Na"
for col in df.columns:
    if col not in ["giá", "số_km_đã_đi"]:
        df[col] = df[col].fillna("Na")

# Xuất file kết quả
df.to_csv("car_data_clean.csv", index=False, encoding="utf-8-sig")

print("✅ Làm sạch xong, số dòng còn lại:", len(df))
