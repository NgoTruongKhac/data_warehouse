# load_to_raw.py
import pandas as pd
import os
import sqlalchemy
from sqlalchemy import text
from dotenv import load_dotenv
import sys
import glob

load_dotenv()

# Config
RAW_TABLE = os.getenv("RAW_TABLE_NAME", "raw_weather_forecast")
OUTPUT_DIR = os.getenv("OUTPUT_DIR")

if not OUTPUT_DIR:
    print("Thiếu OUTPUT_DIR trong .env!")
    sys.exit(1)

engine = sqlalchemy.create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
    pool_pre_ping=True
)

def get_next_batch_id():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COALESCE(MAX(batch_id), 0) + 1 FROM batch_log"))
        return result.scalar()

def log_start(batch_id, file_name, loc_name="", loc_key=""):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO batch_log 
            (batch_id, source_system, source_endpoint, source_file, location_name, location_key, start_time, status)
            VALUES (:bid, 'STAGING', 'load_to_raw', :file, :loc_name, :loc_key, NOW(), 'RUNNING')
        """), {"bid": batch_id, "file": os.path.basename(file_name), "loc_name": loc_name, "loc_key": loc_key})

def log_success(batch_id, count):
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE batch_log SET status='SUCCESS', total_records=:c, success_count=:c, end_time=NOW()
            WHERE batch_id=:bid
        """), {"bid": batch_id, "c": count})

def log_error(batch_id, count, msg):
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE batch_log SET status='FAILED', total_records=:c, end_time=NOW(), 
            error_message=:msg WHERE batch_id=:bid
        """), {"bid": batch_id, "c": count, "msg": str(msg)[:2000]})

def main():
    csv_files = glob.glob(os.path.join(OUTPUT_DIR, "*.csv"))
    if not csv_files:
        print(f"Không tìm thấy file CSV trong: {OUTPUT_DIR}")
        sys.exit(1)

    latest_file = max(csv_files, key=os.path.getmtime)
    print(f"Đang xử lý file: {latest_file}")

    # Đọc CSV
    df = pd.read_csv(latest_file, dtype=str, engine='python', sep=None)
    df.columns = df.columns.str.strip()

    if df.empty:
        print("File CSV rỗng!")
        sys.exit(1)

    print(f"Đã đọc {len(df)} dòng")

    # 1. ĐỔI TÊN CÁC CỘT CHO KHỚP VỚI BẢNG RAW
    rename_map = {
        'date': 'date_time',
        'min_temp': 'min_temp_c', 
        'max_temp': 'max_temp_c'       
    }
    df = df.rename(columns=rename_map)

    # 2. Chỉ giữ lại những cột có trong bảng raw (tránh lỗi cột thừa)
    valid_columns = [
        'date_time', 'location_name', 'location_key', 'min_temp_c', 'max_temp_c',
        'day_icon', 'day_phrase', 'day_precip', 'day_precip_type', 'day_precip_intensity',
        'night_icon', 'night_phrase', 'night_precip', 'night_precip_type', 'night_precip_intensity',
        'source', 'mobile_link', 'link'
    ]
    df = df[[col for col in valid_columns if col in df.columns]]

    # 3. Thêm batch_id
    batch_id = get_next_batch_id()
    df['batch_id'] = batch_id

    # 4. Log bắt đầu
    loc_name = df['location_name'].iloc[0] if 'location_name' in df.columns else "Unknown"
    loc_key = df['location_key'].iloc[0] if 'location_key' in df.columns else "Unknown"
    log_start(batch_id, latest_file, loc_name, loc_key)

    # 5. Load vào bảng raw
    try:
        df.to_sql(
            name=RAW_TABLE,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000,
            dtype={
                'date_time': sqlalchemy.VARCHAR(50),
                'location_name': sqlalchemy.VARCHAR(100),
                'location_key': sqlalchemy.VARCHAR(50),
                'min_temp_c': sqlalchemy.VARCHAR(20),
                'max_temp_c': sqlalchemy.VARCHAR(20),
                'day_icon': sqlalchemy.VARCHAR(10),
                'day_phrase': sqlalchemy.VARCHAR(255),
                'day_precip': sqlalchemy.VARCHAR(10),
                'day_precip_type': sqlalchemy.VARCHAR(50),
                'day_precip_intensity': sqlalchemy.VARCHAR(50),
                'night_icon': sqlalchemy.VARCHAR(10),
                'night_phrase': sqlalchemy.VARCHAR(255),
                'night_precip': sqlalchemy.VARCHAR(10),
                'night_precip_type': sqlalchemy.VARCHAR(50),
                'night_precip_intensity': sqlalchemy.VARCHAR(50),
                'source': sqlalchemy.VARCHAR(100),
                'mobile_link': sqlalchemy.VARCHAR(500),
                'link': sqlalchemy.VARCHAR(500),
                'batch_id': sqlalchemy.BIGINT()
            }
        )
        log_success(batch_id, len(df))
        print(f"HOÀN TẤT! Batch #{batch_id} → {len(df)} bản ghi đã vào {RAW_TABLE}")

    except Exception as e:
        log_error(batch_id, len(df), str(e))
        print(f"LOAD THẤT BẠI: {e}")
        raise

if __name__ == "__main__":
    main()