import pandas as pd

# Đọc file gốc
file_path = "tmdb_movies.csv"   # đổi thành đường dẫn file của bạn
df = pd.read_csv(file_path)

# 1. Loại bỏ các dòng có giá trị bị thiếu (NaN)
df_cleaned = df.dropna()

# 2. Loại bỏ các dòng mà các cột quan trọng bị trống sau khi strip
essential_cols = ["Movie name", "Movie rate", "Description"]
df_cleaned = df_cleaned[df_cleaned[essential_cols].applymap(lambda x: str(x).strip() != "").all(axis=1)]

# 3. Reset lại index
df_cleaned = df_cleaned.reset_index(drop=True)

# 4. (Tuỳ chọn) chuẩn hóa kiểu dữ liệu cho cột rate về dạng float
df_cleaned["Movie rate"] = df_cleaned["Movie rate"].astype(str).str.extract(r"([\d\.]+)").astype(float)

# 5. (Tuỳ chọn) loại bỏ trùng lặp theo tên phim
df_cleaned = df_cleaned.drop_duplicates(subset=["Movie name"], keep="first")

# Lưu ra file mới
cleaned_path = "tmdb_movies_cleaned.csv"
df_cleaned.to_csv(cleaned_path, index=False)

print("✅ Done! File cleaned đã được lưu:", cleaned_path)
