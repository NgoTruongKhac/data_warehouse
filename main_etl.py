import sys
import time

try:
    from schedule_extract import run_scheduler
except ImportError as e:
    print(f"Lỗi: Không thể import script. Đảm bảo các file .py tồn tại. {e}", file=sys.stderr)
    sys.exit(1)


def start_etl_scheduler():
    """
    Chạy Scheduler để tự động thực hiện pipeline Extract theo lịch.
    Người dùng chỉ cần chạy file này là được.
    """

    print("==============================================")
    print("🚀 KHỞI ĐỘNG ETL SCHEDULER 🚀")
    print("==============================================")

    run_scheduler()


# --- Chạy script ---
if __name__ == "__main__":
    start_etl_scheduler()
