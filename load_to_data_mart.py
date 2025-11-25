import os
import sys
import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
import sqlalchemy

# --- CẤU HÌNH & KHỞI TẠO ---
load_dotenv()

# --- Định nghĩa các Constants từ .env ---
WH_DB_NAME = os.getenv("DB_WAREHOUSE_NAME")
FACT_TABLE = os.getenv("FACT_TABLE_NAME")
DIM_DATE = "dim_date" 
LOG_BASE_PATH = os.getenv("LOG_BASE_PATH") 

# Cấu hình 3 Data Mart chuyên biệt (Tên ngắn gọn)
LOCATION_MARTS = {
    "353981": {"name": "ho_chi_minh", "table": "dm_hcm"},
    "353412": {"name": "ha_noi", "table": "dm_hanoi"},
    "427264": {"name": "da_nang", "table": "dm_danang"},
}

AGGREGATE_MART_TABLE = "dm_monthly_summary" 

# ==============================================================================
# HÀM LOG (Ghi ra Console & File)
# ==============================================================================
def log(step, msg, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] [{level}] [{step}] {msg}"
    print(log_line)

    try:
        log_dir = LOG_BASE_PATH
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        with open(os.path.join(log_dir, "load_mart_log.txt"), "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
    except Exception:
        pass

# ==============================================================================
# PHẦN 1: TẠO CẤU TRÚC DATA MART (DDL)
# ==============================================================================
def init_datamart_structure():
    log("P0", "Kiểm tra và khởi tạo cấu trúc Data Mart...", "SETUP")
    
    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    port = int(os.getenv("DB_PORT"))
    dm_name = os.getenv("DM_DB_NAME")

    try:
        conn = pymysql.connect(host=host, user=user, password=password, port=port)
        cursor = conn.cursor()
        
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {dm_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        cursor.execute(f"USE {dm_name};")

        # --- A. TẠO 3 BẢNG DETAIL MARTS ---
        for key, info in LOCATION_MARTS.items():
            table_name = info['table']
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                date_sk INT NOT NULL,
                location_key VARCHAR(50) NOT NULL,
                date_time DATETIME,
                min_temp_c FLOAT,
                max_temp_c FLOAT,
                day_icon INT,
                day_phrase VARCHAR(100),
                day_precip TINYINT(1),
                day_precip_type VARCHAR(20),
                day_precip_intensity VARCHAR(20),
                night_icon INT,
                night_phrase VARCHAR(100),
                night_precip TINYINT(1),
                night_precip_type VARCHAR(20),
                night_precip_intensity VARCHAR(20),
                source VARCHAR(100),
                created_at DATETIME,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (date_sk, location_key)
            );
            """)

        # --- B. TẠO BẢNG AGGREGATE MART (Theo Tháng & Tỉnh) ---
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {AGGREGATE_MART_TABLE} (
            month_sk INT NOT NULL,
            location_key VARCHAR(50) NOT NULL,
            avg_max_temp_c FLOAT,
            avg_min_temp_c FLOAT,
            avg_temp_c FLOAT,
            total_rainy_days INT,
            total_forecast_days INT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (month_sk, location_key)
        );
        """)

        conn.commit()
        cursor.close()
        conn.close()
        log("P0", "Đã hoàn tất tạo 4 bảng Data Mart.", "SUCCESS")

    except Exception as e:
        log("L0", f"Lỗi khởi tạo DB: {e}", "ERROR")
        sys.exit(1)

# ==============================================================================
# PHẦN 2: LOGIC ETL
# ==============================================================================
def get_engine(db_name):
    try:
        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASS')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        uri = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        return create_engine(uri)
    except Exception as e:
        log("CONFIG", f"Lỗi tạo engine cho {db_name}: {e}", "ERROR")
        sys.exit(1)

def main_load_data_mart():
    init_datamart_structure()
    log("START", "Bắt đầu quy trình ETL Data Mart...", "INFO")

    wh_engine = get_engine(WH_DB_NAME) 
    dm_engine = get_engine(os.getenv("DM_DB_NAME")) 

    try:
        # --- [P1] EXTRACT ---
        all_keys = list(LOCATION_MARTS.keys())
        keys_str = ', '.join([f"'{k}'" for k in all_keys])

        # FIX: Thêm d.month_sk vào SELECT để lấy trực tiếp từ Dim Date
        main_query = f"""
        SELECT 
            f.date_sk, f.location_key, f.date_time, f.min_temp_c, f.max_temp_c, 
            f.day_icon, f.day_phrase, f.day_precip, 
            f.night_icon, f.night_phrase, f.night_precip, 
            f.source, f.created_at,
            d.month_sk -- Lấy trực tiếp từ Dimension Date
        FROM {WH_DB_NAME}.{FACT_TABLE} f
        JOIN {WH_DB_NAME}.{DIM_DATE} d ON f.date_sk = d.date_sk
        WHERE f.location_key IN ({keys_str})
        """
        
        df_all = pd.read_sql(main_query, wh_engine)
        log("P1", f"Extract: Đã lấy {len(df_all)} dòng từ Fact Table.", "INFO")

        if df_all.empty:
            log("P1", "Fact Table rỗng. Kết thúc.", "WARNING")
            return

        # --- [P2] TRANSFORM ---
        # 1. Data Quality Check
        df_all.dropna(subset=['date_time', 'min_temp_c', 'max_temp_c', 'date_sk', 'location_key'], inplace=True)
        if df_all.empty:
            log("DQ", "Dữ liệu không hợp lệ sau khi làm sạch. Kết thúc.", "WARNING")
            return
            
        # 2. Tính toán Metrics
        df_all['avg_temp_c'] = (df_all['min_temp_c'] + df_all['max_temp_c']) / 2
        df_all['is_rainy_day'] = df_all.apply(
            lambda x: 1 if (x['day_precip'] == 1 or x['night_precip'] == 1) else 0, axis=1
        )
        
        # KHÔNG CẦN TÍNH month_sk BẰNG PYTHON NỮA (Đã lấy từ SQL)
        # df_all['month_sk'] = pd.to_datetime(df_all['date_time']).dt.strftime('%Y%m').astype(int)
        
        # 3. Xử lý NULL cho text
        text_cols = ['source', 'day_phrase', 'night_phrase', 'created_at']
        for col in text_cols:
            if col in df_all.columns:
                df_all[col] = df_all[col].fillna('')

        # -------------------------------------------------------------
        # --- LOAD (3 BẢNG DETAIL MARTS) ---
        # -------------------------------------------------------------
        log("P2", "Load 3 Detail Marts (Filter & Load)...", "INFO")
        
        # Cột DDL của bảng Detail Mart (Đảm bảo khớp với DDL ở trên)
        DETAIL_COLUMNS = [
            'date_sk', 'location_key', 'date_time', 'min_temp_c', 'max_temp_c', 
            'day_icon', 'day_phrase', 'day_precip', 
            'night_icon', 'night_phrase', 'night_precip', 
            'source', 'created_at'
        ]

        for location_key, info in LOCATION_MARTS.items():
            table_name = info['table']
            df_city = df_all[df_all['location_key'] == location_key].copy()
            
            if df_city.empty:
                continue
            
            # Lọc cột chuẩn trước khi insert
            df_city_filtered = df_city[DETAIL_COLUMNS] 

            with dm_engine.connect() as conn:
                df_city_filtered.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    dtype={'date_time': sqlalchemy.DateTime(), 'created_at': sqlalchemy.DateTime()}
                )
                conn.commit()
            log("P3", f"Hoàn tất load Detail Mart {table_name}.", "SUCCESS")
        
        # -------------------------------------------------------------
        # --- LOAD (BẢNG AGGREGATE MART) ---
        # -------------------------------------------------------------
        log("P2", "Bắt đầu Aggregation theo Tháng & Tỉnh...", "INFO")

        # Aggregation: Tổng hợp theo Tháng VÀ Tỉnh
        df_monthly_city = df_all.groupby(['month_sk', 'location_key']).agg(
            avg_max_temp_c=('max_temp_c', 'mean'),
            avg_min_temp_c=('min_temp_c', 'mean'),
            avg_temp_c=('avg_temp_c', 'mean'), 
            total_rainy_days=('is_rainy_day', 'sum'),
            total_forecast_days=('date_sk', 'nunique')
        ).reset_index()

        log("P3", f"Load {len(df_monthly_city)} dòng vào {AGGREGATE_MART_TABLE}", "INFO")

        # Load vào Database
        with dm_engine.connect() as conn:
            for _, row in df_monthly_city.iterrows():
                sql = text(f"""
                    INSERT INTO {AGGREGATE_MART_TABLE} 
                    (month_sk, location_key, avg_max_temp_c, avg_min_temp_c, avg_temp_c, total_rainy_days, total_forecast_days)
                    VALUES (:ms, :lk, :avg_max, :avg_min, :avg_t, :rain, :total)
                    ON DUPLICATE KEY UPDATE
                        avg_max_temp_c=VALUES(avg_max_temp_c), 
                        avg_min_temp_c=VALUES(avg_min_temp_c), 
                        avg_temp_c=VALUES(avg_temp_c),
                        total_rainy_days=VALUES(total_rainy_days),
                        total_forecast_days=VALUES(total_forecast_days),
                        last_updated=NOW()
                """)
                
                conn.execute(sql, {
                    'ms': row['month_sk'],
                    'lk': row['location_key'],
                    'avg_max': row['avg_max_temp_c'],
                    'avg_min': row['avg_min_temp_c'],
                    'avg_t': row['avg_temp_c'],
                    'rain': row['total_rainy_days'],
                    'total': row['total_forecast_days']
                })
            conn.commit()
        log("P3", f"Hoàn tất load {AGGREGATE_MART_TABLE}.", "SUCCESS")

        log("END", "QUY TRÌNH DATA MART HOÀN TẤT!", "SUCCESS")

    except Exception as e:
        log("ERROR", f"Lỗi ETL Tổng quát: {e}", "ERROR")
        sys.exit(1)
    finally:
        if 'wh_engine' in locals(): wh_engine.dispose()
        if 'dm_engine' in locals(): dm_engine.dispose()

if __name__ == "__main__":
    main_load_data_mart()