import schedule
import time
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import hàm của các bước ETL
from extract_to_file import process_all_endpoints
# from load_to_staging import load_to_staging   # Tương lai
# from transform_data import transform_data     # Tương lai
# from load_to_warehouse import load_wh         # Tương lai


def run_job(job_func, job_name):
    """Hàm chạy 1 job và log trạng thái"""
    print(f"\n🚀 [{job_name}] Bắt đầu lúc: {datetime.now()}")
    try:
        success = job_func()
        if success is True:
            print(f"✅ [{job_name}] Thành công lúc: {datetime.now()}")
        elif success is False:
            print(f"⚠️ [{job_name}] Hoàn thành nhưng có lỗi lúc: {datetime.now()}")
        else:
            print(f"⚠️ [{job_name}] Hoàn thành nhưng không rõ trạng thái (None).")
    except Exception as e:
        print(f"❌ [{job_name}] Lỗi: {e}")
    print("-" * 60)


def schedule_jobs():
    """Khai báo toàn bộ job ETL với lịch chạy cụ thể"""

    # === Job Extract chạy mỗi 2 phút (demo) ===
    schedule.every(2).minutes.do(run_job, process_all_endpoints, "Extract API")

    # === Ví dụ tương lai: chạy lúc 01:00 mỗi ngày ===
    # schedule.every().day.at("01:00").do(run_job, load_to_staging, "Load Staging")

    # === Ví dụ tương lai: chạy lúc 01:15 mỗi ngày ===
    # schedule.every().day.at("01:15").do(run_job, transform_data, "Transform Data")

    # === Ví dụ tương lai: chạy lúc 01:30 mỗi ngày ===
    # schedule.every().day.at("01:30").do(run_job, load_wh, "Load Warehouse")


def run_scheduler():
    """Chạy vòng lặp scheduler"""
    print("=" * 60)
    print("⏰ WEATHER ETL SCHEDULER START")
    print("📌 Nhấn Ctrl + C để dừng!")
    print("=" * 60)

    schedule_jobs()  # 👈 Register tất cả job

    print("📌 Các job đã được đăng ký:")
    for job in schedule.jobs:
        print(f"▶ {job}")

    print("\n🚀 Scheduler đang chạy...")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Scheduler đã dừng!")
