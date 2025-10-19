import React from "react";

export type SymbolOption = { label: string; value: string };

const DEFAULT_SYMBOLS: SymbolOption[] = [
  { label: "BTC/USDT", value: "BTC/USDT" },
  { label: "ETH/USDT", value: "ETH/USDT" },
  { label: "BNB/USDT", value: "BNB/USDT" },
  { label: "SOL/USDT", value: "SOL/USDT" },
  { label: "ADA/USDT", value: "ADA/USDT" },
];

export function SymbolSelect({
  value,
  onChange,
  options = DEFAULT_SYMBOLS,
  disabled = false,
}: {
  value: string | null;
  onChange: (val: string) => void;
  options?: SymbolOption[];
  disabled?: boolean;
}) {
  return (
    <div className="flex items-center gap-2">
      <label className="text-sm font-medium">Symbol</label>
      <select
        className="border rounded-md px-2 py-1"
        value={value ?? ""}
        onChange={(e) => onChange(e.target.value)}
        disabled={disabled}
      >
        <option value="" disabled>
          Select…
        </option>
        {options.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
          </option>
        ))}
      </select>
    </div>
  );
}
