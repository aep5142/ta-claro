export const creditCardOperations = [
  { slug: "purchases", label: "Purchases", operation: "Compras" },
  { slug: "cash-advances", label: "Cash Advances", operation: "Avance en Efectivo" },
  { slug: "fees", label: "Fees", operation: "Cargos por Servicio" },
] as const;

export const operationSlugs = creditCardOperations.map((item) => item.slug);

export type OperationName = (typeof creditCardOperations)[number]["operation"];

export const operationLabelMap: Record<OperationName, string> = {
  Compras: "Purchases",
  "Avance en Efectivo": "Cash Advances",
  "Cargos por Servicio": "Fees",
};

export function operationFromSlug(slug: string): OperationName | null {
  const match = creditCardOperations.find((item) => item.slug === slug);
  return match?.operation ?? null;
}

export const primarySections = [
  { slug: "credit-cards", label: "Credit Cards", href: "/credit-cards/purchases?view=volume" },
  { slug: "debit-cards", label: "Debit Cards", href: "/debit-cards" },
  { slug: "accounts", label: "Accounts", href: "/accounts" },
  { slug: "loans", label: "Loans", href: "/loans" },
] as const;

export const chartViews = [
  { key: "volume", label: "Market Share ($Volume)", metricType: "money" as const },
  { key: "transactions", label: "Market Share (Transactions)", metricType: "count" as const },
  { key: "average-ticket", label: "Average Transaction (CLP)", metricType: "money" as const },
] as const;

export type ChartViewKey = (typeof chartViews)[number]["key"];

export const chartViewByKey = Object.fromEntries(chartViews.map((item) => [item.key, item])) as Record<
  ChartViewKey,
  (typeof chartViews)[number]
>;

export const defaultViewKey: ChartViewKey = "volume";

export function isChartViewKey(value: string | undefined): value is ChartViewKey {
  return Boolean(value && value in chartViewByKey);
}
