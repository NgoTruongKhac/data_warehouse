import express from "express";
import path from "path";
import dotenv from "dotenv";
import { fileURLToPath } from "url";
import router from "./routes/data.route.js";

import { connectDb } from "./database/db.js";

const app = express();
dotenv.config();

const port = process.env.PORT || 3000;

connectDb();

// === Tái tạo __dirname ===
// Trong ES Modules, __dirname không tồn tại sẵn. Ta phải tạo nó từ import.meta.url
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Gọi hàm kết nối DB

// Cấu hình View Engine là EJS
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));

// Cấu hình thư mục Public (file tĩnh)
app.use(express.static(path.join(__dirname, "public")));

// Xử lý dữ liệu từ form
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// Sử dụng Routes
app.get("/", (req, res) => {
  res.render("home");
});
app.use("/weather", router);

// Khởi chạy server
app.listen(port, () => {
  console.log(`Server đang chạy tại http://localhost:${port}`);
});
