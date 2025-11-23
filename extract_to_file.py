import csv
import os
import requests
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv
from location_mapping import get_location_name

# 1. Load biến môi trường
load_dotenv()

# Tạo danh sách các Endpoint cần chạy (Lọc bỏ các giá trị None nếu file env thiếu)
ENDPOINTS = [
    url for url in [
        os.getenv('API_ENDPOINT_HCM'),
        os.getenv('API_ENDPOINT_HN'),
        os.getenv('API_ENDPOINT_DN')
    ] if url # Chỉ lấy giá trị không rỗng
]

OUTPUT_DIR = os.getenv('OUTPUT_DIR')
API_KEY = os.getenv('API_KEY')

def extract_location_key(url):
    """Lấy location_key từ cuối URL endpoint."""
    if not url: return None
    path = urlparse(url).path.rstrip('/')
    return path.split('/')[-1]

def fetch_weather_data(url, api_key):
    """Gọi API lấy dữ liệu JSON."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    location_key = extract_location_key(url)
    print(f"--> Đang gọi API cho Key {location_key}...")
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"    Lỗi API ({location_key}): {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"    Lỗi kết nối ({location_key}): {e}")
        return None

def generate_file_path(base_dir, date_str):
    """
    Tạo tên file dựa trên ngày dự báo đầu tiên.
    Format: weather_yyyy_mmm_dd.csv
    """
    dt = datetime.fromisoformat(date_str)
    file_name = f"weather_{dt.strftime('%Y')}_{dt.strftime('%b')}_{dt.strftime('%d')}.csv"
    return os.path.join(base_dir, file_name)

def process_all_endpoints():
    if not ENDPOINTS or not API_KEY:
        print("Lỗi: Thiếu danh sách ENDPOINT hoặc API_KEY trong .env")
        return

    all_rows = [] # List chứa tất cả dữ liệu của 3 địa điểm
    representative_date = None # Dùng để đặt tên file

    # 2. Vòng lặp qua từng Endpoint
    for url in ENDPOINTS:
        location_key = extract_location_key(url)
        location_name = get_location_name(location_key)
        
        data = fetch_weather_data(url, API_KEY)
        
        if not data:
            continue # Bỏ qua nếu lỗi, chạy tiếp địa điểm sau

        daily_forecasts = data.get('DailyForecasts', [])
        
        # Lấy ngày của địa điểm đầu tiên thành công để làm tên file
        if representative_date is None and daily_forecasts:
            representative_date = daily_forecasts[0].get('Date')

        # Xử lý dữ liệu json thành dictionary phẳng (flat)
        for item in daily_forecasts:
            temp = item.get('Temperature', {})
            day = item.get('Day', {})
            night = item.get('Night', {})

            row = {
                'date': item.get('Date'),
                'location_name': location_name,
                'location_key': location_key,
                'min_temp': temp.get('Minimum', {}).get('Value'),
                'max_temp': temp.get('Maximum', {}).get('Value'),
                
                'day_icon': day.get('Icon'),
                'day_phrase': day.get('IconPhrase'),
                'day_precip': day.get('HasPrecipitation'),
                'day_precip_type': day.get('PrecipitationType', ''),
                'day_precip_intensity': day.get('PrecipitationIntensity', ''),

                'night_icon': night.get('Icon'),
                'night_phrase': night.get('IconPhrase'),
                'night_precip': night.get('HasPrecipitation'),
                'night_precip_type': night.get('PrecipitationType', ''),
                'night_precip_intensity': night.get('PrecipitationIntensity', ''),

                'source': ", ".join(item.get('Sources', [])),
                'mobile_link': item.get('MobileLink'),
                'link': item.get('Link')
            }
            all_rows.append(row)

    # 3. Ghi dữ liệu ra CSV (chỉ ghi nếu có dữ liệu)
    if not all_rows:
        print("Không thu thập được dữ liệu nào.")
        return

    # Tạo thư mục nếu chưa có
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Tạo đường dẫn file output
    output_csv_path = generate_file_path(OUTPUT_DIR, representative_date)

    csv_header = [
        'date', 'location_name', 'location_key', 'min_temp', 'max_temp',
        'day_icon', 'day_phrase', 'day_precip', 'day_precip_type', 'day_precip_intensity',
        'night_icon', 'night_phrase', 'night_precip', 'night_precip_type', 'night_precip_intensity',
        'source', 'mobile_link', 'link'
    ]

    try:
        with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_header)
            writer.writeheader()
            writer.writerows(all_rows) # Ghi tất cả các dòng cùng lúc
        
        print(f"\n--> HOÀN TẤT! Tổng cộng {len(all_rows)} dòng dữ liệu.")
        print(f"--> File lưu tại: {output_csv_path}")
        
    except PermissionError:
        print(f"Lỗi: Không thể ghi file {output_csv_path}. Hãy đóng file nếu đang mở.")

if __name__ == "__main__":
    process_all_endpoints()