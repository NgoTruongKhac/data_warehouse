import os
import sys
import glob
import subprocess
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. Cấu hình và Biến môi trường
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_WAREHOUSE = os.getenv("DB_WAREHOUSE_NAME")
OUTPUT_DIR = os.getenv("OUTPUT_DUMP")
FACT_TABLE = os.getenv("FACT_TABLE_NAME", "fact_weather_forecast")

# Cấu hình đường dẫn tới mysql.exe (nếu chưa có trong PATH)
# Tương tự như bài trước, nếu bạn dùng XAMPP/MySQL Server hãy chỉnh đường dẫn này
MYSQL_EXE_PATH = os.getenv("MYSQL_PATH")
# Ví dụ: MYSQL_EXE_PATH = r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe"

def get_warehouse_engine():
    """Tạo kết nối tới Warehouse DB"""
    try:
        conn_str = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_WAREHOUSE}"
        engine = create_engine(conn_str)
        return engine
    except Exception as e:
        print(f"❌ Lỗi tạo engine: {e}")
        return None

def get_latest_dump_file():
    """Tìm file .sql mới nhất trong thư mục Dumps"""
    if not os.path.exists(OUTPUT_DIR):
        print(f"❌ Thư mục không tồn tại: {OUTPUT_DIR}")
        return None
    
    # Lấy danh sách tất cả file .sql
    list_of_files = glob.glob(os.path.join(OUTPUT_DIR, '*.sql'))
    
    if not list_of_files:
        print("❌ Không tìm thấy file .sql nào trong thư mục.")
        return None
    
    # Tìm file có thời gian tạo mới nhất
    latest_file = max(list_of_files, key=os.path.getctime)
    print(f"📂 Tìm thấy file dump mới nhất: {latest_file}")
    return latest_file

def restore_dump_to_warehouse(dump_file):
    """
    Dùng command line 'mysql' để nạp file dump vào Warehouse.
    Điều này sẽ tạo bảng 'staging_weather_forecast' TẠI Warehouse DB.
    """
    print("⏳ Đang nạp dữ liệu từ Dump vào Warehouse (Staging tạm)...")
    
    # Lệnh: mysql -h... -u... -p... db_name < file.sql
    cmd = [
        MYSQL_EXE_PATH,
        f'--host={DB_HOST}',
        f'--port={DB_PORT}',
        f'--user={DB_USER}',
        DB_WAREHOUSE # Nạp thẳng vào Warehouse DB
    ]

    env_vars = os.environ.copy()
    env_vars['MYSQL_PWD'] = DB_PASS

    try:
        with open(dump_file, 'r') as input_file:
            subprocess.run(cmd, env=env_vars, stdin=input_file, check=True)
        print("✅ Đã nạp xong file Dump vào Warehouse.")
        return True
    except FileNotFoundError:
        print("❌ Lỗi: Không tìm thấy lệnh 'mysql'. Hãy kiểm tra biến MYSQL_EXE_PATH.")
        return False
    except subprocess.CalledProcessError as e:
        print(f"❌ Lỗi khi chạy lệnh mysql restore: {e}")
        return False

def create_fact_table_if_not_exists(engine):
    """Tạo bảng Fact với khóa ngoại liên kết Dim"""
    # SỬA: Đổi date_key thành date_sk để khớp với bảng dim_date
    sql = f"""
    CREATE TABLE IF NOT EXISTS {FACT_TABLE} (
        id_fact BIGINT AUTO_INCREMENT PRIMARY KEY,
        
        -- Khóa ngoại (Foreign Keys)
        date_sk INT NOT NULL,               -- <-- SỬA TÊN CỘT NÀY (date_key -> date_sk)
        location_key VARCHAR(50) NOT NULL,  
        
        -- Các trường dữ liệu từ Staging
        date_time DATETIME,
        min_temp_c FLOAT DEFAULT 0,
        max_temp_c FLOAT DEFAULT 0,
        day_icon INT DEFAULT 0,
        day_phrase VARCHAR(100),
        day_precip BOOLEAN DEFAULT FALSE,
        day_precip_type VARCHAR(20), 
        day_precip_intensity VARCHAR(20),
        night_icon INT DEFAULT 0,
        night_phrase VARCHAR(100),
        night_precip BOOLEAN DEFAULT FALSE,
        night_precip_type VARCHAR(20) ,
        night_precip_intensity VARCHAR(20),
        source VARCHAR(100),
        mobile_link VARCHAR(500), 
        link VARCHAR(500),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        
        -- Định nghĩa rằng buộc (Constraints)
        CONSTRAINT fk_fact_date FOREIGN KEY (date_sk) REFERENCES dim_date(date_sk), -- <-- SỬA THAM CHIẾU NÀY
        CONSTRAINT fk_fact_location FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
    ) ENGINE=InnoDB;
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(sql))
            print(f"✅ Đã kiểm tra/tạo bảng '{FACT_TABLE}'.")
    except Exception as e:
        print(f"❌ Lỗi tạo bảng Fact: {e}")
        sys.exit(1)

def transform_and_load_fact(engine):
    """
    Chuyển dữ liệu từ bảng tạm staging -> bảng Fact.
    SỬA ĐỔI: Join theo full_date thay vì date_sk tự tính.
    """
    sql_etl = f"""
    INSERT INTO {FACT_TABLE} (
        date_sk, location_key, date_time,
        min_temp_c, max_temp_c, day_icon, day_phrase, day_precip,day_precip_type, day_precip_intensity,
        night_icon, night_phrase, night_precip, night_precip_type, night_precip_intensity, source, mobile_link, link
    )
    SELECT 
        d.date_sk,  -- Lấy ID thực tế từ bảng dim_date (ví dụ: 13) thay vì tự tính
        s.location_key,
        s.date_time,
        s.min_temp_c,
        s.max_temp_c,
        s.day_icon,
        s.day_phrase,
        s.day_precip,
        s.day_precip_type,
        s.day_precip_intensity,
        s.night_icon,
        s.night_phrase,
        s.night_precip,
        s.night_precip_type,
        s.night_precip_intensity,
        s.source,
        s.mobile_link,
        s.link
    FROM staging_weather_forecast s
    -- 1. JOIN location (Giữ nguyên)
    JOIN dim_location l ON s.location_key = l.location_key
    
    -- 2. JOIN date (SỬA ĐỔI QUAN TRỌNG)
    -- So sánh ngày trong staging (chuyển về DATE) với cột full_date của dim_date
    JOIN dim_date d ON DATE(s.date_time) = d.full_date
    
    ON DUPLICATE KEY UPDATE
        min_temp_c = VALUES(min_temp_c),
        max_temp_c = VALUES(max_temp_c),
        day_phrase = VALUES(day_phrase),
        night_phrase = VALUES(night_phrase);
    """
    
    sql_drop_temp = "DROP TABLE IF EXISTS staging_weather_forecast;"

    try:
        with engine.connect() as conn:
            print("🔄 Đang chuyển đổi và nạp dữ liệu vào Fact Table...")
            result = conn.execute(text(sql_etl))
            conn.commit()
            print(f"🎉 Đã nạp {result.rowcount} dòng vào '{FACT_TABLE}'.")
            
            print("🧹 Đang dọn dẹp bảng tạm...")
            conn.execute(text(sql_drop_temp))
            conn.commit()
            print("✅ Đã xóa bảng tạm staging_weather_forecast trong Warehouse.")
            
    except Exception as e:
        print(f"❌ Lỗi trong quá trình ETL: {e}")


# --- MAIN ---
if __name__ == "__main__":
    print("🚀 BẮT ĐẦU QUÁ TRÌNH IMPORT DUMP VÀO WAREHOUSE")
    
    # 1. Tìm file dump
    dump_file = get_latest_dump_file()
    if not dump_file:
        sys.exit(1)
        
    # 2. Restore file dump vào Warehouse (tạo bảng staging tạm)
    if not restore_dump_to_warehouse(dump_file):
        sys.exit(1)

    # 3. Kết nối Python với Warehouse để xử lý logic
    engine = get_warehouse_engine()
    if not engine:
        sys.exit(1)

    # 4. Tạo bảng Fact (nếu chưa có)
    create_fact_table_if_not_exists(engine)

    # 5. Transform & Load (Staging -> Fact)
    transform_and_load_fact(engine)
    
    engine.dispose()
    print("\n✅ QUÁ TRÌNH HOÀN TẤT!")