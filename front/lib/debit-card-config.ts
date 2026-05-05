export const debitCardOperations = [
  { slug: "transactions", label: "Debit Transactions", operation: "Debit Transactions" },
  { slug: "atm-withdrawals", label: "ATM Withdrawals", operation: "ATM Withdrawals" },
  {
    slug: "total-activation-rate",
    label: "Operation Metrics",
    operation: "Total Activation Rate",
  },
] as const;

export type DebitOperationName = (typeof debitCardOperations)[number]["operation"];

export function operationFromSlug(slug: string): DebitOperationName | null {
  const match = debitCardOperations.find((item) => item.slug === slug);
  return match?.operation ?? null;
}

export const debitChartViews = [
  {
    key: "volume",
    label: "Volume",
    metricType: "money" as const,
    description: "Monthly debit-card volume for the selected operation.",
    unitLabel: "Millions of CLP. Values are deflated using UF.",
  },
  {
    key: "transactions",
    label: "Transactions",
    metricType: "count" as const,
    description: "Monthly number of operations for the selected debit category.",
    unitLabel: "Number of Operations",
  },
  {
    key: "average-ticket",
    label: "Avg. Transaction",
    metricType: "money" as const,
    description: "Average CLP amount per transaction for the selected debit category.",
    unitLabel: "CLP. Values are deflated using UF.",
  },
  {
    key: "operations-per-active-card",
    label: "Operations per Active Card",
    metricType: "decimal" as const,
    description: "Transactions per active debit/ATM-only card.",
    unitLabel: "Number of Operations",
  },
] as const;

export const debitOperationMetricsViews = [
  {
    key: "total-active-cards",
    label: "Total Active Cards",
    metricType: "count" as const,
    description: "Combined active debit and ATM-only cards per bank and month.",
    unitLabel: "Number of active cards (#).",
  },
  {
    key: "primary-active-cards",
    label: "Primary Active Cards",
    metricType: "count" as const,
    description: "Combined active primary debit and ATM-only cards per bank and month.",
    unitLabel: "Number of active cards (#).",
  },
  {
    key: "supplementary-active-cards",
    label: "Supplementary Active Cards",
    metricType: "count" as const,
    description: "Combined active supplementary debit and ATM-only cards per bank and month.",
    unitLabel: "Number of active cards (#).",
  },
  {
    key: "total-cards-with-operations",
    label: "Total Cards with Operations",
    metricType: "count" as const,
    description: "Combined debit and ATM-only cards with operations in the month.",
    unitLabel: "Number of cards with operations (#).",
  },
  {
    key: "total-activation-rate",
    label: "Total Activation Rate",
    metricType: "ratio" as const,
    description: "Share of active cards that recorded operations in the month.",
    unitLabel: "Percentage of active cards.",
  },
  {
    key: "supplementary-rate",
    label: "Supplementary Rate",
    metricType: "ratio" as const,
    description: "Share of primary cards that have a supplementary card.",
    unitLabel: "Percentage of primary cards.",
  },
] as const;
