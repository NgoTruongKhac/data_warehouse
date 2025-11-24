require('dotenv').config(); // Load biến môi trường từ .env
const mysql = require('mysql2/promise'); // Sử dụng phiên bản hỗ trợ Promise (Async/Await)

// Tạo Pool kết nối (Tốt cho Web Server)
const pool = mysql.createPool({
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASS,
    database: process.env.DM_DB_NAME, // Kết nối vào 'weather_data_mart'
    port: parseInt(process.env.DB_PORT) || 3306,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Kiểm tra kết nối
pool.getConnection()
    .then(conn => {
        console.log("✅ Đã kết nối thành công đến MySQL Data Mart!");
        conn.release();
    })
    .catch(err => {
        console.error("❌ Lỗi kết nối MySQL:", err.message);
    });

module.exports = pool;