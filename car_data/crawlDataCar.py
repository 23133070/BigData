import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

baseUrl = "https://oto.com.vn"
headers = {"User-Agent": "Mozilla/5.0"}

# Hàm đổi giá về số
def convertPrice(price: str):
    try:
        result = 0
        if "tỉ" in price:
            temp1 = price.split(" tỉ ")
            result += int(temp1[0]) * 10**9
            if len(temp1) > 1 and temp1[1].split()[0].isdigit():
                result += int(temp1[1].split()[0]) * 10**6
        else:
            result += int(price.split()[0]) * 10**6
        return result
    except:
        return None


# Hàm lấy chi tiết xe
def crawl_car_detail(link: str):
    url = baseUrl + link
    try:
        response = requests.get(url, headers=headers, timeout=20)
        if response.status_code != 200:
            print(f"Link lỗi {response.status_code}: {url}")
            return None

        soup = BeautifulSoup(response.content, "html.parser")
        car_data = {}

        # ==== Tên xe và giá ====
        try:
            carTitle = soup.select_one(".group-title-detail > .title-detail").get_text().strip()
            if " - " in carTitle:
                parts = carTitle.split(" - ")
                car_data["Tên xe"] = parts[0].strip()
                car_data["Giá"] = convertPrice(parts[1].strip())
            else:
                car_data["Tên xe"] = carTitle
                price_tag = soup.select_one(".price")
                car_data["Giá"] = convertPrice(price_tag.get_text().strip()) if price_tag else None
        except:
            car_data["Tên xe"] = None
            price_tag = soup.select_one(".price")
            car_data["Giá"] = convertPrice(price_tag.get_text().strip()) if price_tag else None

        # ==== Các trường thông tin khác ====
        fields = {
            "năm": "Năm sản xuất",
            "kiểu": "Kiểu dáng",
            "trạng": "Tình trạng",
            "xuất": "Xuất xứ",
            "km": "Số km đã đi",
            "tỉnh": "Tỉnh thành",
            "hộp": "Hộp số",
            "nhiên": "Nhiên liệu"
        }
        for f in fields.values():
            car_data[f] = None

        carInfos = soup.select(".box-info-detail > .list-info > li")
        for carInfo in carInfos:
            text = carInfo.get_text().lower()
            for key, field in fields.items():
                if key in text:
                    car_data[field] = carInfo.get_text().split(":")[-1].strip()

        return car_data

    except Exception as e:
        print(f"Lỗi khi crawl {url}: {e}")
        return None


# Crawl danh sách xe từ nhiều trang
def crawl_all(max_pages=10, delay=1):
    all_data = []
    for i in range(1, max_pages + 1):
        try:
            list_url = f"{baseUrl}/mua-ban-xe-cu-da-qua-su-dung/p{i}"
            response = requests.get(list_url, headers=headers, timeout=20)
            soup = BeautifulSoup(response.content, "html.parser")

            links = [a["href"] for a in soup.select(".item-car > .photo > a") if a.get("href")]
            print(f"Trang {i}: lấy được {len(links)} link")

            for link in links:
                car_data = crawl_car_detail(link)
                if car_data:
                    all_data.append(car_data)
                time.sleep(delay)
        except Exception as e:
            print(f"Lỗi trang {i}: {e}")

    return all_data


# === Chạy crawl ===
if __name__ == "__main__":
    data = crawl_all(max_pages=150, delay=1)
    df = pd.DataFrame(data)
    df.to_csv("oto_data.csv", encoding="utf-8-sig", index=False)
    print(f"Crawl xong, tổng số xe: {len(df)}")
