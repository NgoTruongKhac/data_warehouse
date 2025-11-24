import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# --- Cấu hình kết nối (Tương tự code trước) ---
def get_staging_engine():
    load_dotenv()
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_name = os.getenv("DB_NAME") # Kết nối vào Staging DB

    if not all([db_host, db_port, db_user, db_pass, db_name]):
        print("❌ Lỗi: Thiếu biến môi trường.", file=sys.stderr)
        return None

    try:
        conn_str = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(conn_str)
        return engine
    except Exception as e:
        print(f"❌ Lỗi tạo engine: {e}")
        return None

# --- Câu lệnh SQL Upsert ---
UPSERT_SQL = text("""
INSERT INTO staging_weather_forecast (
    batch_id, location_key, location_name, date_time,
    min_temp_c, max_temp_c, 
    day_icon, day_phrase, day_precip, day_precip_type, day_precip_intensity,
    night_icon, night_phrase, night_precip, night_precip_type, night_precip_intensity,
    source, mobile_link, link,
    created_at,
    is_update, date_update
)
SELECT 
    batch_id, location_key, location_name, date_time,
    min_temp_c, max_temp_c, 
    day_icon, day_phrase, day_precip, day_precip_type, day_precip_intensity,
    night_icon, night_phrase, night_precip, night_precip_type, night_precip_intensity,
    source, mobile_link, link,
    created_at,
    0, NULL  -- is_update=FALSE (0), date_update=NULL
FROM transform_weather_forecast
ON DUPLICATE KEY UPDATE
    batch_id = VALUES(batch_id),
    location_name = VALUES(location_name),
    min_temp_c = VALUES(min_temp_c),
    max_temp_c = VALUES(max_temp_c),
    day_icon = VALUES(day_icon),
    day_phrase = VALUES(day_phrase),
    day_precip = VALUES(day_precip),
    day_precip_type = VALUES(day_precip_type),
    day_precip_intensity = VALUES(day_precip_intensity),
    night_icon = VALUES(night_icon),
    night_phrase = VALUES(night_phrase),
    night_precip = VALUES(night_precip),
    night_precip_type = VALUES(night_precip_type),
    night_precip_intensity = VALUES(night_precip_intensity),
    source = VALUES(source),
    mobile_link = VALUES(mobile_link),
    link = VALUES(link),
    is_update = 1,          -- is_update=TRUE
    date_update = NOW();    -- date_update=Current Time
""")

# --- Hàm đảm bảo Unique Key ---
def ensure_unique_key(connection):
    """
    Kiểm tra và tạo Unique Key nếu chưa có. 
    Cần thiết để ON DUPLICATE KEY UPDATE hoạt động đúng.
    """
    check_sql = text("""
        SELECT COUNT(1) 
        FROM information_schema.statistics 
        WHERE table_schema = DATABASE() 
          AND table_name = 'staging_weather_forecast' 
          AND index_name = 'uq_forecast';
    """)
    
    result = connection.execute(check_sql).scalar()
    
    if result == 0:
        print("⚠️ Chưa có Unique Key. Đang tạo index 'uq_forecast'...")
        try:
            # Lưu ý: Dữ liệu hiện tại phải sạch (không trùng) thì lệnh này mới chạy được
            connection.execute(text("""
                ALTER TABLE staging_weather_forecast 
                ADD UNIQUE KEY uq_forecast (location_key, date_time);
            """))
            print("✅ Đã tạo Unique Key thành công.")
        except Exception as e:
            print(f"❌ Không thể tạo Unique Key (có thể do dữ liệu đang bị trùng lặp): {e}")
            raise e
    else:
        print("ℹ️ Unique Key 'uq_forecast' đã tồn tại.")

# --- Main Script ---
def run_etl_load_staging():
    engine = get_staging_engine()
    if not engine: return

    print("🚀 Bắt đầu quá trình Load từ Transform -> Staging...")
    
    try:
        with engine.connect() as conn:
            # 1. Đảm bảo điều kiện tiên quyết
            ensure_unique_key(conn)
            
            # 2. Thực thi Upsert
            print("⏳ Đang thực thi lệnh UPSERT...")
            result = conn.execute(UPSERT_SQL)
            conn.commit()
            
            # 3. Thông báo kết quả
            print(f"✅ Hoàn tất! Số dòng bị ảnh hưởng (Inserted/Updated): {result.rowcount}")
            # Lưu ý: MySQL trả về rowcount = 1 cho Insert, = 2 cho Update
            
    except SQLAlchemyError as e:
        print(f"❌ Lỗi SQL: {e}")
    except Exception as e:
        print(f"❌ Lỗi không xác định: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    run_etl_load_staging()