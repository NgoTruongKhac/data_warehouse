import mysql from "mysql2/promise"; // Thay đổi require thành import
import dotenv from "dotenv";

dotenv.config(); // Nạp biến môi trường

// 1. Tạo Connection Pool
// Pool giúp tái sử dụng kết nối, tốt hơn cho hiệu năng web server
export const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT || 3306,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// 2. Hàm connectDb để kiểm tra kết nối khi server khởi động
export const connectDb = async () => {
  try {
    // Thử lấy một kết nối từ pool để kiểm tra
    const connection = await pool.getConnection();
    console.log("✅ Kết nối MySQL thành công!");
    connection.release(); // Trả kết nối về pool ngay sau khi kiểm tra xong
  } catch (error) {
    console.error("❌ Kết nối MySQL thất bại:", error.message);
    // Có thể throw error nếu muốn dừng server khi không có DB
    // process.exit(1);
  }
};

// Trong ES Modules, ta dùng export trực tiếp ở đầu biến hoặc dùng export {} như dưới đây (nếu chưa export lẻ tẻ ở trên)
// Nhưng ở trên tôi đã dùng 'export const', nên không cần module.exports nữa.
