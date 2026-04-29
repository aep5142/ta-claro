export function formatMoney(value: number): string {
  return new Intl.NumberFormat("de-DE", {
    maximumFractionDigits: 0,
    minimumFractionDigits: 0,
  }).format(Math.round(value));
}

export function formatDecimal(value: number): string {
  return new Intl.NumberFormat("de-DE", {
    maximumFractionDigits: 2,
    minimumFractionDigits: 2,
  }).format(value);
}

export function formatPercent(value: number): string {
  return `${new Intl.NumberFormat("de-DE", {
    maximumFractionDigits: 1,
    minimumFractionDigits: 1,
  }).format(value)}%`;
}

export function formatMonthLabel(month: string): string {
  const [year, rawMonth] = month.split("-");
  return `${rawMonth}/${year.slice(2)}`;
}

export function parseMonthValue(month: string): Date {
  const [year, rawMonth] = month.split("-");
  return new Date(Date.UTC(Number(year), Number(rawMonth) - 1, 1));
}

export function normalizeMonthValue(month: string): string {
  const [year, rawMonth] = month.split("-");
  return `${year}-${rawMonth.padStart(2, "0")}`;
}

export function addMonths(date: Date, count: number): Date {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + count, 1));
}

export function buildMonthOptions(startMonth: string, endMonth: string): string[] {
  if (!startMonth || !endMonth) {
    return [];
  }

  const values: string[] = [];
  let cursor = parseMonthValue(startMonth);
  const limit = parseMonthValue(endMonth);

  while (cursor <= limit) {
    values.push(cursor.toISOString().slice(0, 7));
    cursor = addMonths(cursor, 1);
  }

  return values;
}

export function calculateMarketShares(value: number, total: number): number {
  if (!total) {
    return 0;
  }

  return (value / total) * 100;
}

export function getChileTodayIso(): string {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Santiago",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });

  return formatter.format(new Date());
}
