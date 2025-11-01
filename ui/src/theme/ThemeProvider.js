import { jsx as _jsx } from "react/jsx-runtime";
import { createContext, useContext, useEffect, useMemo, useState } from "react";
const ThemeContext = createContext(undefined);
const PREFERENCE_KEY = "app.theme-preference";
const getSystemMode = () => {
    if (typeof window === "undefined") {
        return "dark";
    }
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
};
const applyMode = (mode) => {
    const root = document.documentElement;
    root.dataset.theme = mode;
};
export function ThemeProvider({ children }) {
    const [systemMode, setSystemMode] = useState(() => getSystemMode());
    const [preference, setPreference] = useState(() => {
        if (typeof window === "undefined") {
            return "system";
        }
        const stored = window.localStorage.getItem(PREFERENCE_KEY);
        if (stored === "light" || stored === "dark" || stored === "system") {
            return stored;
        }
        return "system";
    });
    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }
        const media = window.matchMedia("(prefers-color-scheme: dark)");
        const handleChange = () => {
            setSystemMode(media.matches ? "dark" : "light");
        };
        handleChange();
        media.addEventListener("change", handleChange);
        return () => media.removeEventListener("change", handleChange);
    }, []);
    const effectiveMode = preference === "system" ? systemMode : preference;
    useEffect(() => {
        applyMode(effectiveMode);
    }, [effectiveMode]);
    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }
        window.localStorage.setItem(PREFERENCE_KEY, preference);
    }, [preference]);
    const value = useMemo(() => ({
        mode: effectiveMode,
        systemMode,
        preference,
        setPreference,
        toggle: () => {
            setPreference((prev) => {
                if (prev === "system") {
                    return systemMode === "dark" ? "light" : "dark";
                }
                return prev === "dark" ? "light" : "dark";
            });
        },
    }), [effectiveMode, preference, systemMode]);
    return _jsx(ThemeContext.Provider, { value: value, children: children });
}
export function useTheme() {
    const ctx = useContext(ThemeContext);
    if (!ctx) {
        throw new Error("useTheme must be used within ThemeProvider");
    }
    return ctx;
}
