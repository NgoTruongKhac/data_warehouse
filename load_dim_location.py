import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# --- DỮ LIỆU ĐẦU VÀO ---
LOCATIONS_DATA = [
    {"key": "353981", "name": "Ho Chi Minh"},
    {"key": "353412", "Ha Noi": "Ha Noi"}, # Lưu ý: Data gốc bạn đưa key hơi lạ, mình chuẩn hóa lại bên dưới
    {"key": "427264", "name": "Da Nang"}
]

# Chuẩn hóa lại format data cho dễ xử lý (List of Dictionary chuẩn)
CLEAN_DATA = [
    {"location_key": "353981", "location_name": "Ho Chi Minh"},
    {"location_key": "353412", "location_name": "Ha Noi"},
    {"location_key": "427264", "location_name": "Da Nang"}
]

# --- HÀM KẾT NỐI (Tương tự các bài trước) ---
def get_warehouse_engine():
    load_dotenv()
    
    # Lấy thông tin kết nối
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    
    # Ưu tiên lấy tên Warehouse, nếu không có thì lấy DB_NAME, nếu không có nữa thì báo lỗi
    db_name = os.getenv("DB_WAREHOUSE_NAME") or os.getenv("DB_NAME")

    if not all([db_host, db_port, db_user, db_pass, db_name]):
        print("❌ Lỗi: Thiếu thông tin cấu hình trong .env", file=sys.stderr)
        return None

    try:
        # Tạo chuỗi kết nối SQLAlchemy
        conn_str = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(conn_str)
        print(f"🔌 Đã kết nối tới Database: {db_name}")
        return engine
    except Exception as e:
        print(f"❌ Lỗi tạo engine: {e}")
        return None

# --- HÀM TẠO BẢNG (DDL) ---
def create_table_if_not_exists(conn):
    sql = text("""
    CREATE TABLE IF NOT EXISTS dim_location (
        location_key VARCHAR(50) NOT NULL,
        location_name VARCHAR(100) NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (location_key)
    ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """)
    conn.execute(sql)
    print("✅ Đã kiểm tra/tạo bảng 'dim_location'.")

# --- HÀM NẠP DỮ LIỆU (UPSERT) ---
def upsert_locations(engine, data):
    upsert_sql = text("""
    INSERT INTO dim_location (location_key, location_name)
    VALUES (:location_key, :location_name)
    ON DUPLICATE KEY UPDATE
        location_name = VALUES(location_name);
    """)

    try:
        with engine.connect() as conn:
            # 1. Tạo bảng trước
            create_table_if_not_exists(conn)
            
            # 2. Thực thi Upsert cho từng dòng
            print(f"🔄 Đang đồng bộ {len(data)} địa điểm...")
            for row in data:
                conn.execute(upsert_sql, row)
            
            conn.commit()
            print("🎉 Đồng bộ dim_location thành công!")
            
    except SQLAlchemyError as e:
        print(f"❌ Lỗi SQL: {e}")

# --- MAIN ---
if __name__ == "__main__":
    engine = get_warehouse_engine()
    if engine:
        upsert_locations(engine, CLEAN_DATA)
        engine.dispose()