const UNIT_ORDER: Record<string, number> = {
  s: 0,
  m: 1,
  h: 2,
  d: 3,
  w: 4,
  mo: 5,
};

export function formatTimeframeLabel(key: string): string {
  if (/^[a-z]?[\d]+$/i.test(key)) {
    const unit = key.replace(/\d+/g, "");
    const magnitude = key.replace(/[^\d]/g, "");
    if (!unit) {
      return `${magnitude}`;
    }
    const normalized = unit.toLowerCase();
    if (normalized === "m" || normalized === "h") {
      return `${magnitude}${normalized}`;
    }
  }
  return key.replace(/_/g, " ").toUpperCase();
}

function parseForSort(key: string): { weight: number; magnitude: number } {
  const unit = key.replace(/\d+/g, "").toLowerCase();
  const magnitude = Number(key.replace(/[^\d]/g, "")) || 0;
  const weight = UNIT_ORDER[unit] ?? 999;
  return { weight, magnitude };
}

export function sortTimeframes(keys: string[]): string[] {
  return [...new Set(keys)]
    .filter((key) => key)
    .sort((a, b) => {
      const left = parseForSort(a);
      const right = parseForSort(b);
      if (left.weight === right.weight) {
        return left.magnitude - right.magnitude;
      }
      return left.weight - right.weight;
    });
}
