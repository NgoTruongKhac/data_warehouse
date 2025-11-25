import express from "express";
import {
  getDataWeather,
  getDataWeatherSummary,
} from "../controllers/data.controller.js";

const router = express.Router();

// Route tĩnh phải đặt trước
router.get("/summary", getDataWeatherSummary);

// Route động lấy tham số :location
router.get("/:location", getDataWeather);

export default router;
