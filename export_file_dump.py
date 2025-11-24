import os
import sys
import subprocess
from datetime import datetime
from dotenv import load_dotenv

def export_table_to_sql():
    # 1. Tải biến môi trường
    load_dotenv()

    mysql_dump_path = os.getenv("MYSQL_DUMP_PATH")
    
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_name = os.getenv("DB_NAME")
    output_dir = os.getenv("OUTPUT_DUMP")

    # Kiểm tra biến môi trường
    if not all([db_host, db_port, db_user, db_pass, db_name, output_dir]):
        print("❌ Lỗi: Thiếu thông tin trong file .env")
        return

    # 2. Chuẩn bị thư mục đầu ra
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"📁 Đã tạo thư mục: {output_dir}")
    except OSError as e:
        print(f"❌ Lỗi tạo thư mục: {e}")
        return

    # 3. Tạo tên file output (kèm thời gian)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = "staging_weather_forecast"
    filename = f"{table_name}_{timestamp}.sql"
    output_path = os.path.join(output_dir, filename)

    print(f"🚀 Bắt đầu export bảng '{table_name}'...")
    print(f"   Database: {db_name}")
    print(f"   Output: {output_path}")

    # 4. Cấu hình lệnh mysqldump
    # Lưu ý: Cần đảm bảo 'mysqldump' đã được thêm vào PATH của Windows/Linux
    dump_cmd = [
        mysql_dump_path,
        f'--host={db_host}',
        f'--port={db_port}',
        f'--user={db_user}',
        '--no-tablespaces',       # Tránh lỗi quyền truy cập tablespace
        '--column-statistics=0',  # Fix lỗi version MySQL 8.0+
        '--quick',                # Đọc từng dòng, tốt cho bảng lớn
        '--lock-tables=false',    # Không khóa bảng (nếu DB đang hoạt động)
        db_name,
        table_name                # Chỉ export bảng này
    ]

    # 5. Thực thi
    # Sử dụng biến môi trường cho password để an toàn hơn (tránh cảnh báo password in command line)
    env_vars = os.environ.copy()
    env_vars['MYSQL_PWD'] = db_pass

    try:
        with open(output_path, 'w', encoding='utf-8') as outfile:
            subprocess.run(
                dump_cmd, 
                env=env_vars, 
                stdout=outfile, 
                check=True,  # Sẽ ném lỗi nếu mysqldump thất bại
                text=True
            )
        print(f"✅ Export thành công! File lưu tại:\n   👉 {output_path}")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Lỗi khi chạy mysqldump (Exit code {e.returncode}).")
        print("💡 Gợi ý: Kiểm tra xem đã cài MySQL và thêm vào biến môi trường PATH chưa.")
    except FileNotFoundError:
        print("❌ Lỗi: Không tìm thấy lệnh 'mysqldump'.")
        print("💡 Hãy cài đặt MySQL Server/Client hoặc thêm đường dẫn thư mục bin của MySQL vào System PATH.")
    except Exception as e:
        print(f"❌ Lỗi không xác định: {e}")

if __name__ == "__main__":
    export_table_to_sql()