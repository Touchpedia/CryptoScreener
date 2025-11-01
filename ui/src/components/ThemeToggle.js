import { jsxs as _jsxs, jsx as _jsx } from "react/jsx-runtime";
import { useTheme } from "../theme/ThemeProvider";
export default function ThemeToggle() {
    const { preference, mode, toggle, setPreference, systemMode } = useTheme();
    const isSystem = preference === "system";
    return (_jsxs("div", { className: "theme-toggle", role: "group", "aria-label": "Theme selector", children: [_jsxs("button", { type: "button", className: isSystem ? "active" : "secondary", onClick: () => setPreference("system"), children: ["System (", systemMode, ")"] }), _jsx("button", { type: "button", className: !isSystem && mode === "light" ? "active" : "secondary", onClick: () => setPreference("light"), children: "Light" }), _jsx("button", { type: "button", className: !isSystem && mode === "dark" ? "active" : "secondary", onClick: () => setPreference("dark"), children: "Dark" }), _jsx("button", { type: "button", className: "icon", onClick: toggle, "aria-label": "Toggle theme", children: "?" })] }));
}
