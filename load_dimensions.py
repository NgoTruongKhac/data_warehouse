import os
import sys
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Date, String, VARCHAR
from sqlalchemy.exc import SQLAlchemyError
import pymysql

# --- HÀM HELPER: Lấy Engine (Đã sửa đổi để linh hoạt) ---
def get_db_engine(db_name_env_key):
    """
    Tạo Engine dựa trên key của tên database trong file .env
    Ví dụ: db_name_env_key='DB_STAGING_NAME' hoặc 'DB_WAREHOUSE_NAME'
    """
    load_dotenv()
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    
    # Lấy tên DB động dựa trên tham số truyền vào
    db_name = os.getenv(db_name_env_key)
    
    if not all([db_host, db_port, db_user, db_pass, db_name]):
        print(f"Lỗi: Thiếu thông tin cấu hình cho '{db_name_env_key}' trong .env", file=sys.stderr)
        return None # Trả về None để xử lý ở hàm main thay vì exit ngay

    # 1. Tạo CSDL nếu chưa có (dùng PyMySql)
    conn = None
    try:
        conn = pymysql.connect(
            user=db_user,
            password=db_pass,
            host=db_host,
            port=int(db_port),
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        # Tạo DB nếu chưa tồn tại
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        print(f"✅ CSDL '{db_name}' đã sẵn sàng.")
    except pymysql.MySQLError as err:
        print(f"❌ Lỗi PyMySql khi tạo CSDL {db_name}: {err}", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn:
            conn.close()

    # 2. Tạo Engine (dùng SQLAlchemy)
    try:
        connection_string = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        return engine
    except SQLAlchemyError as e:
        print(f"❌ Lỗi SQLAlchemy khi tạo Engine cho {db_name}: {e}", file=sys.stderr)
        sys.exit(1)

# --- HÀM CHÍNH: Tải Dimension (Giữ nguyên logic) ---
def load_dimension(engine, file_name, table_name, schema, pk_column):
    """
    Tải dữ liệu từ một file CSV vào database được chỉ định bởi engine.
    """
    if not engine:
        print("⚠️ Engine không hợp lệ, bỏ qua.")
        return

    if not os.path.exists(file_name):
        print(f"❌ LỖI: Không tìm thấy file '{file_name}'.", file=sys.stderr)
        return
    
    column_names = list(schema.keys())
    
    # Lấy tên database từ engine để in log cho rõ
    db_name = engine.url.database
    print(f"⬇️  Đang xử lý cho Database: {db_name}")

    try:
        print(f"   - Đọc file: {file_name}...")
        df = pd.read_csv(
            file_name,
            header=None,
            names=column_names
        )
        
        print(f"   - Đang tải {len(df)} dòng vào bảng '{table_name}'...")
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            dtype=schema,
            chunksize=1000
        )
        
        print(f"   - Đang tạo Khóa chính ({pk_column})...")
        with engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_column});"))
            conn.commit()
            
        print(f"✅ Thành công: Bảng '{table_name}' tại DB '{db_name}'.\n")

    except SQLAlchemyError as e:
        print(f"❌ Lỗi SQL khi tải bảng '{table_name}' vào '{db_name}': {e}", file=sys.stderr)
    except Exception as e:
        print(f"❌ Lỗi không xác định: {e}", file=sys.stderr)

# --- Schemas ---
DATE_SCHEMA = {
    'date_sk': Integer,
    'full_date': Date,
    'day_since_2005': Integer,
    'month_sk': Integer,
    'day_name': VARCHAR(20),
    'month_name': VARCHAR(20),
    'year': Integer,
    'year_month': VARCHAR(10),
    'day_of_month': Integer,
    'day_of_year': Integer,
    'week_of_year_sunday': Integer,
    'year_week_sunday': VARCHAR(10),
    'week_sunday_start': Date,
    'week_of_year_monday': Integer,
    'year_week_monday': VARCHAR(10),
    'week_monday_start': Date,
    'holiday_flag': VARCHAR(20),
    'day_type': VARCHAR(20)
}

# --- Chạy Script ---
if __name__ == "__main__":
    print("==============================================")
    print("🚀 BẮT ĐẦU TẢI DIM_DATE CHO CẢ 2 HỆ THỐNG")
    print("==============================================")
    
    # 1. Cấu hình cho Staging
    print("--- 1. Kết nối Staging DB ---")
    staging_engine = get_db_engine("DB_NAME") # Lấy tên từ biến DB_NAME
    
    # 2. Cấu hình cho Warehouse
    print("--- 2. Kết nối Warehouse DB ---")
    wh_engine = get_db_engine("DB_WAREHOUSE_NAME")    # Lấy tên từ biến DB_WAREHOUSE_NAME
    
    file_csv = 'date_dim_without_quarter.csv'

    # 3. Thực thi tải dữ liệu cho Staging
    if staging_engine:
        load_dimension(
            engine=staging_engine,
            file_name=file_csv,
            table_name='dim_date',  
            schema=DATE_SCHEMA,
            pk_column='date_sk'
        )

    # 4. Thực thi tải dữ liệu cho Warehouse
    if wh_engine:
        load_dimension(
            engine=wh_engine,
            file_name=file_csv,
            table_name='dim_date',  
            schema=DATE_SCHEMA,
            pk_column='date_sk'
        )
        
    print("==============================================")
    print("🎉 HOÀN TẤT QUÁ TRÌNH.")
    print("==============================================")
    
    # Dọn dẹp
    if staging_engine: staging_engine.dispose()
    if wh_engine: wh_engine.dispose()