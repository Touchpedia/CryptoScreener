import AppShell from "./components/AppShell";
import { ThemeProvider } from "./theme/ThemeProvider";
import "./App.css";

export default function App() {
  return (
    <ThemeProvider>
      <AppShell />
    </ThemeProvider>
  );
}
