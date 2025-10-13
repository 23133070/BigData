import pandas as pd, numpy as np, re, os, glob

BASE_DIR = r"D:\bigdata3"
output_dir = os.path.join(BASE_DIR, "StandardizedData")
os.makedirs(output_dir, exist_ok=True)

# Tự tìm file csv phù hợp
cands = []
for pat in [
    os.path.join(BASE_DIR, "tmdb_movies_cleaned.csv"),
    os.path.join(BASE_DIR, "tmdb_movies_clean.csv"),
    os.path.join(BASE_DIR, "tmdb_movies.csv"),
]:
    cands.extend(glob.glob(pat))
if not cands:
    raise FileNotFoundError("Không thấy CSV trong D:\\bigdata3 (vd: tmdb_movies_cleaned.csv).")
input_file = cands[0]
print("👉 Đang đọc file tại:", input_file)

# ====== helper ======
def clean_int_like(s):
    if pd.isna(s): return np.nan
    digits = re.sub(r"[^\d\-]", "", str(s))
    if not digits or digits == "-": return np.nan
    try: return int(digits)
    except: return pd.to_numeric(digits, errors="coerce")

def clean_float_like(s):
    if pd.isna(s): return np.nan
    x = str(s).strip()
    if x == "": return np.nan
    if "," in x and "." in x: x = x.replace(",", "")
    elif "," in x: x = x.replace(",", ".")
    return pd.to_numeric(x, errors="coerce")

def parse_date_any(s): return pd.to_datetime(s, errors="coerce", infer_datetime_format=True)
def title_fix_caps(t): return t.title() if isinstance(t,str) and t.isupper() else t
def normalize_bool_col(series):
    truthy, falsy = {"true","t","1","y","yes"}, {"false","f","0","n","no"}
    s = series.astype("string").str.strip().str.lower()
    m = s.isin(truthy|falsy)
    if m.any():
        out = np.where(s.isin(truthy), True, np.where(s.isin(falsy), False, pd.NA))
        return pd.Series(out, index=series.index, dtype="boolean")
    return series

# ====== read ======
df = pd.read_csv(input_file, encoding="utf-8", engine="python")

# ====== minimal normalize ======
for c in ["title","original_title"]:
    if c in df.columns: df[c] = df[c].astype(str).str.strip()

if "release_date" in df.columns:
    df["release_date"] = df["release_date"].astype(str).str.strip()
    df = df[(df["release_date"]!="") | (df.get("title", pd.Series(dtype=str))!="")]

for c in ["budget","revenue","popularity","vote_average","runtime"]:
    if c in df.columns: df[c] = df[c].apply(clean_float_like)
if "vote_count" in df.columns: df["vote_count"] = df["vote_count"].apply(clean_int_like)

if "release_date" in df.columns:
    p = parse_date_any(df["release_date"])
    df["release_date_parsed"] = p
    df["release_year"] = p.dt.year
    df = df[(df["release_year"].isna()) | ((df["release_year"]>=1900)&(df["release_year"]<=2025))]

if "budget" in df.columns:
    df.loc[df["budget"]==0,"budget"]=np.nan
    df = df[(df["budget"].isna()) | ((df["budget"]>=0)&(df["budget"]<=1e12))]
if "revenue" in df.columns:
    df.loc[df["revenue"]==0,"revenue"]=np.nan
    df = df[(df["revenue"].isna()) | ((df["revenue"]>=0)&(df["revenue"]<=1e13))]
if "runtime" in df.columns: df = df[(df["runtime"].isna()) | ((df["runtime"]>=1)&(df["runtime"]<=500))]
if "vote_average" in df.columns: df = df[(df["vote_average"].isna()) | ((df["vote_average"]>=0)&(df["vote_average"]<=10))]
if "vote_count" in df.columns: df = df[(df["vote_count"].isna()) | (df["vote_count"]>=0)]
if "popularity" in df.columns: df = df[(df["popularity"].isna()) | (df["popularity"]>=0)]

for col in ["title","original_title","status","original_language","overview","tagline"]:
    if col in df.columns: df[col] = df[col].astype(str).str.strip()
if "title" in df.columns: df["title"] = df["title"].apply(title_fix_caps)
for bcol in ["adult","video"]:
    if bcol in df.columns: df[bcol] = normalize_bool_col(df[bcol])

if "release_date" in df.columns:
    df["release_date_iso"] = parse_date_any(df["release_date"]).dt.strftime("%Y-%m-%d")

# ====== write ======
out_csv = os.path.join(output_dir, "tmdb_movies_clean_standardized.csv")
out_parquet = os.path.join(output_dir, "tmdb_movies_normalized.parquet")

df.to_csv(out_csv, index=False, encoding="utf-8-sig")
try:
    df.to_parquet(out_parquet, index=False, engine="pyarrow")
except Exception:
    df.to_parquet(out_parquet, index=False)

print("✅ Chuẩn hóa xong!")
print("📂 CSV:", out_csv)
print("📂 Parquet:", out_parquet)
