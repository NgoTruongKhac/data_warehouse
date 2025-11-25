import { getCityWeather, getMonthlySummary } from "../models/weather.model.js";

// Map mã tỉnh thành sang tên bảng Database
const locationMap = {
  hn: "dm_hanoi",
  dn: "dm_danang",
  hcm: "dm_hcm", // Ví dụ thêm
};

// Map tên hiển thị
const locationNameMap = {
  hn: "Hà Nội",
  dn: "Đà Nẵng",
  hcm: "TP. Hồ Chí Minh",
};

export const getDataWeather = async (req, res) => {
  try {
    const { location } = req.params;
    const tableName = locationMap[location];

    // Kiểm tra nếu location không hợp lệ
    if (!tableName) {
      return res
        .status(404)
        .send("Địa điểm không tồn tại hoặc chưa được hỗ trợ.");
    }

    const data = await getCityWeather(tableName);

    // Render view và truyền dữ liệu
    res.render("dataWeather", {
      data: data,
      locationCode: location,
      locationName: locationNameMap[location],
      pageTitle: `Thời tiết ${locationNameMap[location]}`,
    });
  } catch (error) {
    console.error(error);
    res.status(500).send("Lỗi server khi lấy dữ liệu thời tiết.");
  }
};

export const getDataWeatherSummary = async (req, res) => {
  try {
    const data = await getMonthlySummary();

    res.render("dataWeatherSummary", {
      data: data,
      pageTitle: "Tổng hợp thời tiết hàng tháng",
    });
  } catch (error) {
    console.error(error);
    res.status(500).send("Lỗi server khi lấy báo cáo tổng hợp.");
  }
};
