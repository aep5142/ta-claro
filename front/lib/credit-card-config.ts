export const creditCardOperations = [
  { slug: "purchases", label: "Purchases", operation: "Compras" },
  { slug: "cash-advances", label: "Cash Advances", operation: "Avance en Efectivo" },
  { slug: "fees", label: "Fees", operation: "Cargos por Servicio" },
  { slug: "operations-rate", label: "Operations Rate", operation: "Operations Rate" },
] as const;

export const operationSlugs = creditCardOperations.map((item) => item.slug);

export type OperationName = (typeof creditCardOperations)[number]["operation"];

export const operationLabelMap: Record<OperationName, string> = {
  Compras: "Purchases",
  "Avance en Efectivo": "Cash Advances",
  "Cargos por Servicio": "Fees",
  "Operations Rate": "Operations Rate",
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
  {
    key: "volume",
    label: "Volume",
    metricType: "money" as const,
    description: "Monthly credit-card volume for the selected operation.",
    unitLabel: "Millions of CLP. Values are deflated using UF.",
  },
  {
    key: "transactions",
    label: "Transactions",
    metricType: "count" as const,
    description: "Monthly number of card operations for the selected operation.",
    unitLabel: "Number of Operations",
  },
  {
    key: "average-ticket",
    label: "Avg. Transaction",
    metricType: "money" as const,
    description: "Average CLP amount per transaction for the selected operation.",
    unitLabel: "CLP. Values are deflated using UF.",
  },
  {
    key: "operations-per-active-card",
    label: "Operations per Active Card",
    metricType: "decimal" as const,
    description: "Transactions per active credit card.",
    unitLabel: "Number of Operations",
  },
] as const;

export const operationsRateViews = [
  {
    key: "total-active-cards",
    label: "Total Active Cards",
    metricType: "count" as const,
    description: "Active credit cards per bank and month.",
    unitLabel: "Number of active cards (#).",
  },
  {
    key: "total-cards-with-operations",
    label: "Total Cards with Operations",
    metricType: "count" as const,
    description: "Credit cards that registered at least one operation in the month.",
    unitLabel: "Number of cards with operations (#).",
  },
  {
    key: "operations-rate",
    label: "Operations Rate",
    metricType: "ratio" as const,
    description: "Share of active cards that recorded operations in the month.",
    unitLabel: "Percentage of active cards.",
  },
] as const;

export type ChartViewKey = (typeof chartViews)[number]["key"];
export type OperationsRateViewKey = (typeof operationsRateViews)[number]["key"];

export const chartViewByKey = Object.fromEntries(chartViews.map((item) => [item.key, item])) as Record<
  ChartViewKey,
  (typeof chartViews)[number]
>;

export const operationsRateViewByKey = Object.fromEntries(
  operationsRateViews.map((item) => [item.key, item])
) as Record<OperationsRateViewKey, (typeof operationsRateViews)[number]>;

export const defaultViewKey: ChartViewKey = "volume";
export const defaultOperationsRateViewKey: OperationsRateViewKey = "total-active-cards";

export function isChartViewKey(value: string | undefined): value is ChartViewKey {
  return Boolean(value && value in chartViewByKey);
}

export function isOperationsRateViewKey(value: string | undefined): value is OperationsRateViewKey {
  return Boolean(value && value in operationsRateViewByKey);
}

export function isOperationsRateOperation(operation: OperationName): operation is "Operations Rate" {
  return operation === "Operations Rate";
}
