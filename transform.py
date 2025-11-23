# transform.py - ĐÚNG THEO WORKFLOW MỚI NHẤT (có kiểm tra raw trước, log lỗi rõ ràng)
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from dotenv import load_dotenv
import os
import sys
from datetime import datetime

load_dotenv()

# ============ CẤU HÌNH ============
RAW_TABLE = "raw_weather_forecast"
TRANSFORM_TABLE = "transform_weather_forecast"
BATCH_LOG_TABLE = "batch_log"

engine = sqlalchemy.create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
    pool_pre_ping=True
)

# ============ HELPER LOG ============
def print_log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")

def get_current_batch_id():
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT DISTINCT batch_id FROM {RAW_TABLE} LIMIT 1"))
        row = result.fetchone()
        return row[0] if row else None

def update_batch_status(batch_id, status, error_msg=None, clean_count=None):
    with engine.begin() as conn:
        if status == "SUCCESS":
            conn.execute(text(f"""
                UPDATE {BATCH_LOG_TABLE} SET 
                    status = 'SUCCESS',
                    success_count = :cnt,
                    total_records = (SELECT COUNT(*) FROM {RAW_TABLE} WHERE batch_id = :bid),
                    end_time = NOW()
                WHERE batch_id = :bid
            """), {"bid": batch_id, "cnt": clean_count})
        elif status == "FAILED":
            msg = str(error_msg)[:2000] if error_msg else "Lỗi không xác định"
            conn.execute(text(f"""
                UPDATE {BATCH_LOG_TABLE} SET 
                    status = 'FAILED',
                    error_message = :msg,
                    end_time = NOW()
                WHERE batch_id = :bid
            """), {"bid": batch_id, "msg": msg})

# ============ MAIN WORKFLOW ============
def main():
    print_log("=== BẮT ĐẦU TRANSFORM THEO WORKFLOW MỚI ===")

    # 1. Kết nối database
    print_log("Kết nối đến weather_staging_db")

    # 2. Kiểm tra raw_weather_forecast có dữ liệu không?
    batch_id = get_current_batch_id()
    
    if not batch_id:
        error_msg = "Không có dữ liệu trong bảng raw_weather_forecast"
        print_log(f"THẤT BẠI: {error_msg}")
        print_log("=== KẾT THÚC VỚI LỖI ===")
        sys.exit(1)  # Thoát ngay, không làm gì thêm

    print_log(f"Phát hiện batch_id = {batch_id} → Có dữ liệu raw → Tiếp tục")

    try:
        # 3. TRUNCATE bảng transform
        print_log(f"TRUNCATE bảng {TRANSFORM_TABLE}")
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {TRANSFORM_TABLE}"))

        # 4. Đọc và convert dữ liệu từ raw
        print_log(f"Đọc dữ liệu từ {RAW_TABLE} (batch_id = {batch_id})")
        df = pd.read_sql(f"SELECT * FROM {RAW_TABLE} WHERE batch_id = {batch_id}", engine)
        raw_count = len(df)
        print_log(f"Đọc được {raw_count:,} bản ghi thô")

        # ================== CONVERT TỪNG FIELD ==================
        def safe_datetime(x):
            try:
                s = str(x).split('+')[0].split('Z')[0].split('.')[0]
                return pd.to_datetime(s)
            except:
                return pd.NaT

        df['date_time'] = df['date_time'].apply(safe_datetime)
        df['min_temp_c'] = pd.to_numeric(df['min_temp_c'], errors='coerce')
        df['max_temp_c'] = pd.to_numeric(df['max_temp_c'], errors='coerce')

        df['day_icon'] = pd.to_numeric(df['day_icon'], errors='coerce').fillna(0).astype(int)
        df['night_icon'] = pd.to_numeric(df['night_icon'], errors='coerce').fillna(0).astype(int)

        df['day_phrase'] = df['day_phrase'].fillna('Unknown').str.slice(0, 100)
        df['night_phrase'] = df['night_phrase'].fillna('Unknown').str.slice(0, 100)

        df['day_precip'] = df['day_precip'].notnull() & (df['day_precip'].astype(str).str.strip() != '0')
        df['night_precip'] = df['night_precip'].notnull() & (df['night_precip'].astype(str).str.strip() != '0')

        df['day_precip_type'] = df['day_precip_type'].fillna('None').str.slice(0, 20)
        df['day_precip_intensity'] = df['day_precip_intensity'].fillna('None').str.slice(0, 20)
        df['night_precip_type'] = df['night_precip_type'].fillna('None').str.slice(0, 20)
        df['night_precip_intensity'] = df['night_precip_intensity'].fillna('None').str.slice(0, 20)

        df['location_name'] = df['location_name'].fillna('Ho Chi Minh City')
        df['location_key'] = df['location_key'].fillna('353981')
        df['source'] = df['source'].fillna('AccuWeather')

        # Loại bỏ dòng lỗi nghiêm trọng
        before = len(df)
        df = df.dropna(subset=['date_time', 'min_temp_c', 'max_temp_c'])
        dropped = before - len(df)
        if dropped > 0:
            print_log(f"Loại bỏ {dropped} dòng không hợp lệ (thiếu ngày/nhiệt độ)")

        clean_count = len(df)

        # 5. Ghi dữ liệu sạch vào transform
        print_log(f"Ghi {clean_count:,} bản ghi sạch vào {TRANSFORM_TABLE}")
        df.to_sql(
            name=TRANSFORM_TABLE,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000,
            dtype={
                'batch_id': sqlalchemy.BIGINT(),
                'date_time': sqlalchemy.DATETIME(),
                'location_key': sqlalchemy.VARCHAR(50),
                'location_name': sqlalchemy.VARCHAR(100),
                'min_temp_c': sqlalchemy.FLOAT(),
                'max_temp_c': sqlalchemy.FLOAT(),
                'day_icon': sqlalchemy.INTEGER(),
                'day_phrase': sqlalchemy.VARCHAR(100),
                'day_precip': sqlalchemy.BOOLEAN(),
                'day_precip_type': sqlalchemy.VARCHAR(20),
                'day_precip_intensity': sqlalchemy.VARCHAR(20),
                'night_icon': sqlalchemy.INTEGER(),
                'night_phrase': sqlalchemy.VARCHAR(100),
                'night_precip': sqlalchemy.BOOLEAN(),
                'night_precip_type': sqlalchemy.VARCHAR(20),
                'night_precip_intensity': sqlalchemy.VARCHAR(20),
                'source': sqlalchemy.VARCHAR(100),
                'mobile_link': sqlalchemy.VARCHAR(500),
                'link': sqlalchemy.VARCHAR(500),
            }
        )

        # 6. Ghi log thành công vào batch_log
        update_batch_status(batch_id, "SUCCESS", clean_count=clean_count)
        print_log(f"Ghi log thành công vào batch_log (batch_id = {batch_id})")

        # 7. Cuối cùng mới TRUNCATE raw
        print_log(f"TRUNCATE bảng {RAW_TABLE} (dọn dữ liệu thô)")
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {RAW_TABLE}"))

        print_log("=== HOÀN TẤT TOÀN BỘ QUY TRÌNH TRANSFORM THÀNH CÔNG! ===")

    except Exception as e:
        error_detail = f"Transform lỗi: {str(e)}"
        print_log(f"THẤT BẠI: {error_detail}")
        if batch_id:
            update_batch_status(batch_id, "FAILED", error_msg=error_detail)
            print_log(f"Đã ghi lỗi vào batch_log (batch_id = {batch_id})")
        print_log("=== KẾT THÚC VỚI LỖI ===")
        raise

if __name__ == "__main__":
    main()