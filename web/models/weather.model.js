import { pool } from "../database/db.js";

// Lấy dữ liệu chi tiết theo tỉnh thành
export const getCityWeather = async (tableName) => {
  // Lưu ý: Tên bảng không thể dùng làm tham số (?) trong Prepared Statement
  // Nên ta phải validate kỹ tên bảng trước khi gọi hàm này
  const query = `SELECT * FROM ${tableName} ORDER BY date_time ASC`;
  const [rows] = await pool.query(query);
  return rows;
};

// Lấy dữ liệu tổng hợp tháng
export const getMonthlySummary = async () => {
  // Sắp xếp theo tháng tăng dần, sau đó đến mã tỉnh
  const query =
    "SELECT * FROM dm_monthly_summary ORDER BY month_sk ASC, location_key ASC";
  const [rows] = await pool.query(query);
  return rows;
};
