import { useTheme } from "../theme/ThemeProvider";

export default function ThemeToggle() {
  const { preference, mode, toggle, setPreference, systemMode } = useTheme();
  const isSystem = preference === "system";

  return (
    <div className="theme-toggle" role="group" aria-label="Theme selector">
      <button
        type="button"
        className={isSystem ? "active" : "secondary"}
        onClick={() => setPreference("system")}
      >
        System ({systemMode})
      </button>
      <button
        type="button"
        className={!isSystem && mode === "light" ? "active" : "secondary"}
        onClick={() => setPreference("light")}
      >
        Light
      </button>
      <button
        type="button"
        className={!isSystem && mode === "dark" ? "active" : "secondary"}
        onClick={() => setPreference("dark")}
      >
        Dark
      </button>
      <button type="button" className="icon" onClick={toggle} aria-label="Toggle theme">
        ?
      </button>
    </div>
  );
}
