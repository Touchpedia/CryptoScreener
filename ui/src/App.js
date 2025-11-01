import { jsx as _jsx } from "react/jsx-runtime";
import AppShell from "./components/AppShell";
import { ThemeProvider } from "./theme/ThemeProvider";
import "./App.css";
export default function App() {
    return (_jsx(ThemeProvider, { children: _jsx(AppShell, {}) }));
}
