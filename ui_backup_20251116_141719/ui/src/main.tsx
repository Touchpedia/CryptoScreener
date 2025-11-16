import React from "react";
import { createRoot } from "react-dom/client";
import App from "./App";

const el = document.getElementById("root");
if (!el) {
  const d = document.createElement("div");
  d.id = "root";
  document.body.appendChild(d);
  createRoot(d).render(<App />);
} else {
  createRoot(el).render(<App />);
}
