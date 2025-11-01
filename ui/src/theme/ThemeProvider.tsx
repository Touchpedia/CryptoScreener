import { createContext, ReactNode, useContext, useEffect, useMemo, useState } from "react";

type ThemeMode = "light" | "dark";
type ThemePreference = ThemeMode | "system";

type ThemeContextValue = {
  mode: ThemeMode;
  systemMode: ThemeMode;
  preference: ThemePreference;
  setPreference: (value: ThemePreference) => void;
  toggle: () => void;
};

const ThemeContext = createContext<ThemeContextValue | undefined>(undefined);

const PREFERENCE_KEY = "app.theme-preference";

const getSystemMode = (): ThemeMode => {
  if (typeof window === "undefined") {
    return "dark";
  }
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
};

const applyMode = (mode: ThemeMode) => {
  const root = document.documentElement;
  root.dataset.theme = mode;
};

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [systemMode, setSystemMode] = useState<ThemeMode>(() => getSystemMode());
  const [preference, setPreference] = useState<ThemePreference>(() => {
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

  const value = useMemo<ThemeContextValue>(
    () => ({
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
    }),
    [effectiveMode, preference, systemMode],
  );

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme() {
  const ctx = useContext(ThemeContext);
  if (!ctx) {
    throw new Error("useTheme must be used within ThemeProvider");
  }
  return ctx;
}
