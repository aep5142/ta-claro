"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { BankSelector } from "@/components/bank-selector";
import { EmptyState, ErrorState, LoadingState } from "@/components/dashboard-states";
import { MetricLineChart } from "@/components/metric-line-chart";
import { getBankDisplayName } from "@/lib/bank-presentation";
import {
  type CreditCardMetricRow,
  fetchCreditCardMetrics,
  fetchDatasetBoundary,
  fetchLatestUfValue,
  fetchOperationsRateBoundary,
  fetchOperationsRateMetrics,
  type OperationsRateMetricRow,
} from "@/lib/supabase-queries";
import {
  chartViews,
  defaultOperationsRateViewKey,
  defaultViewKey,
  isChartViewKey,
  isOperationsRateOperation,
  isOperationsRateViewKey,
  operationLabelMap,
  operationsRateViews,
  type ChartViewKey,
  type OperationName,
  type OperationsRateViewKey,
} from "@/lib/credit-card-config";
import {
  addMonths,
  buildMonthOptions,
  calculateMarketShares,
  formatDecimal,
  formatMoney,
  formatMonthLabel,
  formatPercent,
  getChileTodayIso,
  normalizeMonthValue,
  parseMonthValue,
} from "@/lib/formatters";
import { cn } from "@/lib/utils";

type CreditCardsDashboardProps = {
  operation: OperationName;
  operationSlug: string;
  initialView?: string;
};

type BoundaryState = {
  earliestMonth: string;
  latestMonth: string;
  defaultUfValue: number | null;
  defaultUfDate: string | null;
};

type MetricType = "money" | "count" | "decimal" | "ratio";

export function CreditCardsDashboard({
  operation,
  initialView,
}: CreditCardsDashboardProps) {
  const isOperationsRateDashboard = isOperationsRateOperation(operation);
  const initialMetricKey = isOperationsRateDashboard
    ? isOperationsRateViewKey(initialView)
      ? initialView
      : defaultOperationsRateViewKey
    : isChartViewKey(initialView)
      ? initialView
      : defaultViewKey;

  const [viewKey, setViewKey] = useState<ChartViewKey | OperationsRateViewKey>(initialMetricKey);
  const [boundaryState, setBoundaryState] = useState<BoundaryState | null>(null);
  const [startMonth, setStartMonth] = useState("");
  const [endMonth, setEndMonth] = useState("");
  const [operationRows, setOperationRows] = useState<CreditCardMetricRow[]>([]);
  const [operationsRateRows, setOperationsRateRows] = useState<OperationsRateMetricRow[]>([]);
  const [selectedBanks, setSelectedBanks] = useState<string[]>([]);
  const [ufInput, setUfInput] = useState("");
  const [isLoadingBounds, setIsLoadingBounds] = useState(true);
  const [isLoadingRows, setIsLoadingRows] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const hasSeededSelectionRef = useRef(false);

  useEffect(() => {
    setViewKey(
      isOperationsRateDashboard
        ? isOperationsRateViewKey(initialView)
          ? initialView
          : defaultOperationsRateViewKey
        : isChartViewKey(initialView)
          ? initialView
          : defaultViewKey
    );
  }, [initialView, isOperationsRateDashboard]);

  useEffect(() => {
    let isCancelled = false;

    async function loadBoundaries() {
      setIsLoadingBounds(true);
      setErrorMessage(null);
      setOperationRows([]);
      setOperationsRateRows([]);

      try {
        if (isOperationsRateDashboard) {
          const [latestMonth, earliestMonth] = await Promise.all([
            fetchOperationsRateBoundary("latest"),
            fetchOperationsRateBoundary("earliest"),
          ]);

          if (!latestMonth || !earliestMonth) {
            throw new Error("No operations-rate data is available.");
          }

          const boundedStart = normalizeMonthValue(
            addMonths(parseMonthValue(latestMonth), -11).toISOString().slice(0, 7)
          );
          const safeStart =
            boundedStart < earliestMonth.slice(0, 7) ? earliestMonth.slice(0, 7) : boundedStart;

          if (!isCancelled) {
            setBoundaryState({
              earliestMonth: earliestMonth.slice(0, 7),
              latestMonth: latestMonth.slice(0, 7),
              defaultUfDate: null,
              defaultUfValue: null,
            });
            setStartMonth(safeStart);
            setEndMonth(latestMonth.slice(0, 7));
            setUfInput("");
            hasSeededSelectionRef.current = false;
          }

          return;
        }

        const chileToday = getChileTodayIso();
        const [latestMonth, earliestMonth, latestUf] = await Promise.all([
          fetchDatasetBoundary(operation, "latest"),
          fetchDatasetBoundary(operation, "earliest"),
          fetchLatestUfValue(chileToday),
        ]);

        if (!latestMonth || !earliestMonth) {
          throw new Error("No credit-card data is available for this operation.");
        }

        const boundedStart = normalizeMonthValue(
          addMonths(parseMonthValue(latestMonth), -11).toISOString().slice(0, 7)
        );
        const safeStart =
          boundedStart < earliestMonth.slice(0, 7) ? earliestMonth.slice(0, 7) : boundedStart;

        if (!isCancelled) {
          setBoundaryState({
            earliestMonth: earliestMonth.slice(0, 7),
            latestMonth: latestMonth.slice(0, 7),
            defaultUfDate: latestUf.ufDate,
            defaultUfValue: latestUf.value,
          });
          setStartMonth(safeStart);
          setEndMonth(latestMonth.slice(0, 7));
          setUfInput(formatMoney(Math.round(latestUf.value)));
          hasSeededSelectionRef.current = false;
        }
      } catch (error) {
        if (!isCancelled) {
          setErrorMessage(error instanceof Error ? error.message : "Unable to load dashboard metadata.");
        }
      } finally {
        if (!isCancelled) {
          setIsLoadingBounds(false);
        }
      }
    }

    void loadBoundaries();

    return () => {
      isCancelled = true;
    };
  }, [isOperationsRateDashboard, operation]);

  useEffect(() => {
    if (!startMonth || !endMonth) {
      return;
    }

    let isCancelled = false;

    async function loadRows() {
      setIsLoadingRows(true);
      setErrorMessage(null);

      try {
        if (isOperationsRateDashboard) {
          const nextRows = await fetchOperationsRateMetrics(`${startMonth}-01`, `${endMonth}-01`);

          if (!nextRows.length) {
            throw new Error("The selected time range returned no rows.");
          }

          if (!isCancelled) {
            setOperationsRateRows(nextRows);
            setOperationRows([]);
          }
          return;
        }

        const nextRows = await fetchCreditCardMetrics(operation, `${startMonth}-01`, `${endMonth}-01`);

        if (!nextRows.length) {
          throw new Error("The selected time range returned no rows.");
        }

        if (!isCancelled) {
          setOperationRows(nextRows);
          setOperationsRateRows([]);
        }
      } catch (error) {
        if (!isCancelled) {
          setOperationRows([]);
          setOperationsRateRows([]);
          setErrorMessage(error instanceof Error ? error.message : "Unable to load credit-card metrics.");
        }
      } finally {
        if (!isCancelled) {
          setIsLoadingRows(false);
        }
      }
    }

    void loadRows();

    return () => {
      isCancelled = true;
    };
  }, [endMonth, isOperationsRateDashboard, operation, startMonth]);

  const activeUfValue = useMemo(() => {
    const parsed = Number(ufInput.replace(/\./g, "").replace(",", "."));
    return Number.isFinite(parsed) && parsed > 0 ? parsed : boundaryState?.defaultUfValue ?? 0;
  }, [boundaryState?.defaultUfValue, ufInput]);

  const months = useMemo(() => buildMonthOptions(startMonth, endMonth), [startMonth, endMonth]);

  const activeMetric = isOperationsRateDashboard
    ? operationsRateViews.find((item) => item.key === viewKey) ?? operationsRateViews[0]
    : chartViews.find((item) => item.key === viewKey) ?? chartViews[0];

  const loadedMonthKeys = useMemo(
    () =>
      Array.from(
        new Set(
          (isOperationsRateDashboard ? operationsRateRows : operationRows).map((row) =>
            row.period_month.slice(0, 7)
          )
        )
      ).sort(),
    [isOperationsRateDashboard, operationRows, operationsRateRows]
  );
  const latestLoadedMonth = loadedMonthKeys.at(-1) ?? null;

  const bankSeries = useMemo(() => {
    const grouped = new Map<string, Record<string, number | null>>();
    const bankNames = new Map<string, string>();

    if (isOperationsRateDashboard) {
      operationsRateRows.forEach((row) => {
        const monthKey = row.period_month.slice(0, 7);
        const metricValue = getOperationsRateMetricValue(
          row,
          viewKey as OperationsRateViewKey
        );

        const bankMonths =
          grouped.get(row.institution_code) ??
          (Object.fromEntries(months.map((month) => [month, null])) as Record<string, number | null>);
        bankMonths[monthKey] = metricValue;
        grouped.set(row.institution_code, bankMonths);
        bankNames.set(row.institution_code, row.institution_name);
      });
    } else {
      operationRows.forEach((row) => {
        const monthKey = row.period_month.slice(0, 7);
        const metricValue = getOperationMetricValue(
          row,
          viewKey as ChartViewKey,
          activeUfValue
        );

        const bankMonths =
          grouped.get(row.institution_code) ??
          (Object.fromEntries(months.map((month) => [month, null])) as Record<string, number | null>);
        bankMonths[monthKey] = metricValue;
        grouped.set(row.institution_code, bankMonths);
        bankNames.set(row.institution_code, row.institution_name);
      });
    }

    return Array.from(grouped.entries())
      .map(([institutionCode, series]) => ({
        institutionCode,
        institutionName: getBankDisplayName(bankNames.get(institutionCode) ?? institutionCode),
        series,
      }))
      .sort((left, right) => left.institutionName.localeCompare(right.institutionName));
  }, [activeUfValue, isOperationsRateDashboard, months, operationRows, operationsRateRows, viewKey]);

  useEffect(() => {
    if (!bankSeries.length) {
      setSelectedBanks([]);
      hasSeededSelectionRef.current = false;
      return;
    }

    setSelectedBanks((current) => {
      const availableCodes = new Set(bankSeries.map((bank) => bank.institutionCode));
      const filtered = current.filter((code) => availableCodes.has(code));

      if (filtered.length) {
        return filtered;
      }

      if (hasSeededSelectionRef.current || !latestLoadedMonth) {
        return current;
      }

      const ranked = bankSeries
        .map((bank) => ({
          institutionCode: bank.institutionCode,
          value: bank.series[latestLoadedMonth],
        }))
        .filter((bank): bank is { institutionCode: string; value: number } => typeof bank.value === "number")
        .sort((left, right) => right.value - left.value)
        .slice(0, 5)
        .map((item) => item.institutionCode);

      hasSeededSelectionRef.current = true;
      return ranked;
    });
  }, [bankSeries, latestLoadedMonth]);

  const selectedSeries = useMemo(
    () => bankSeries.filter((bank) => selectedBanks.includes(bank.institutionCode)),
    [bankSeries, selectedBanks]
  );

  const latestMonthRows = useMemo(
    () =>
      (isOperationsRateDashboard ? operationsRateRows : operationRows).filter(
        (row) => latestLoadedMonth !== null && row.period_month.slice(0, 7) === latestLoadedMonth
      ),
    [isOperationsRateDashboard, latestLoadedMonth, operationRows, operationsRateRows]
  );

  const summaryRows = useMemo(() => {
    const bankMap = new Map(selectedSeries.map((bank) => [bank.institutionCode, bank.institutionName] as const));

    const totals = latestMonthRows.reduce(
      (accumulator, row) => {
        if (isOperationsRateDashboard) {
          return accumulator;
        }

        accumulator.volume += Number((row as CreditCardMetricRow).real_value_uf) * activeUfValue;
        accumulator.transactions += Number((row as CreditCardMetricRow).transaction_count);
        return accumulator;
      },
      { volume: 0, transactions: 0 }
    );

    const selectedRows = selectedBanks
      .map((institutionCode) => {
        const row = latestMonthRows.find((item) => item.institution_code === institutionCode);
        if (!row) {
          return null;
        }

        const currentValue = isOperationsRateDashboard
          ? getOperationsRateMetricValue(row as OperationsRateMetricRow, viewKey as OperationsRateViewKey)
          : getOperationMetricValue(row as CreditCardMetricRow, viewKey as ChartViewKey, activeUfValue);

        if (currentValue === null) {
          return null;
        }

        const share =
          isOperationsRateDashboard ||
          viewKey === "average-ticket" ||
          viewKey === "operations-per-active-card"
            ? null
            : calculateMarketShares(
                viewKey === "volume"
                  ? Number((row as CreditCardMetricRow).real_value_uf) * activeUfValue
                  : Number((row as CreditCardMetricRow).transaction_count),
                viewKey === "volume" ? totals.volume : totals.transactions
              );

        return {
          institutionCode,
          institutionName: bankMap.get(institutionCode) ?? getBankDisplayName(row.institution_name),
          currentValue,
          share,
        };
      })
      .filter((row): row is NonNullable<typeof row> => Boolean(row))
      .sort((left, right) => right.currentValue - left.currentValue);

    if (isOperationsRateDashboard || viewKey === "average-ticket" || viewKey === "operations-per-active-card") {
      return selectedRows;
    }

    const selectedTotal = selectedRows.reduce((accumulator, row) => accumulator + row.currentValue, 0);
    const totalValue = viewKey === "volume" ? totals.volume : totals.transactions;
    const othersValue = Math.max(totalValue - selectedTotal, 0);
    const othersShare = calculateMarketShares(othersValue, totalValue);

    return [
      ...selectedRows,
      {
        institutionCode: "others",
        institutionName: "Others",
        currentValue: othersValue,
        share: othersShare,
      },
    ];
  }, [activeUfValue, isOperationsRateDashboard, latestMonthRows, selectedBanks, selectedSeries, viewKey]);

  const availableMonthOptions = useMemo(() => {
    if (!boundaryState) {
      return [];
    }

    return buildMonthOptions(boundaryState.earliestMonth, boundaryState.latestMonth);
  }, [boundaryState]);

  if (isLoadingBounds) {
    return <LoadingState label="Loading dashboard configuration" />;
  }

  if (errorMessage && !operationRows.length && !operationsRateRows.length) {
    return <ErrorState title="Unable to load the dashboard" description={errorMessage} />;
  }

  if (!boundaryState) {
    return <EmptyState title="No data available" description="No operation metadata was returned." />;
  }

  if (!latestLoadedMonth && !isLoadingRows) {
    return <EmptyState title="No loaded data" description="The selected time range did not return any complete points." />;
  }

  return (
    <section className="space-y-6">
      <div className="rounded-3xl border border-border bg-panel p-6">
        <div className="flex flex-col gap-6 xl:flex-row xl:items-end xl:justify-between">
          <div className="max-w-3xl">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-brand">Credit Cards</p>
            <h1 className="mt-3 text-3xl font-semibold text-white">{operationLabelMap[operation]}</h1>
            <p className="mt-3 text-sm leading-6 text-muted">
              Monthly credit-card analysis across banks, with UF-adjusted CLP values for volume and ticket metrics, plus the active-card and operations-rate views.
            </p>
          </div>

          <div className={cn("grid gap-3", isOperationsRateDashboard ? "sm:grid-cols-2" : "sm:grid-cols-3")}>
            <ControlCard label="Start (MM/YY)">
              <select
                value={startMonth}
                onChange={(event) => setStartMonth(event.target.value)}
                className="w-full rounded-2xl border border-border bg-panelMuted px-3 py-2 text-sm text-white outline-none transition focus:border-brand/60"
              >
                {availableMonthOptions.map((month) => (
                  <option key={month} value={month} disabled={month > endMonth}>
                    {formatMonthLabel(month)}
                  </option>
                ))}
              </select>
            </ControlCard>
            <ControlCard label="End (MM/YY)">
              <select
                value={endMonth}
                onChange={(event) => setEndMonth(event.target.value)}
                className="w-full rounded-2xl border border-border bg-panelMuted px-3 py-2 text-sm text-white outline-none transition focus:border-brand/60"
              >
                {availableMonthOptions.map((month) => (
                  <option key={month} value={month} disabled={month < startMonth}>
                    {formatMonthLabel(month)}
                  </option>
                ))}
              </select>
            </ControlCard>
            {!isOperationsRateDashboard ? (
              <ControlCard label="UF today">
                <div className="relative">
                  <span className="pointer-events-none absolute inset-y-0 left-3 flex items-center text-sm text-muted">$</span>
                  <input
                    value={ufInput}
                    onChange={(event) => {
                      const digits = event.target.value.replace(/\D/g, "");
                      setUfInput(digits ? formatMoney(Number(digits)) : "");
                    }}
                    inputMode="numeric"
                    className="w-full rounded-2xl border border-border bg-panelMuted py-2 pl-8 pr-3 text-sm text-white outline-none transition focus:border-brand/60"
                  />
                </div>
              </ControlCard>
            ) : null}
          </div>
        </div>
      </div>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_420px]">
        <div className="space-y-6">
          <div className="rounded-3xl border border-border bg-panel p-6">
            <div className="mb-6 flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
              <div>
                <h2 className="text-xl font-semibold text-white">{activeMetric.label}</h2>
                <p className="mt-2 text-xs italic text-muted">
                  {activeMetric.key === "volume" ? "Millions of CLP" : activeMetric.unitLabel}
                </p>
              </div>

              <div className="flex flex-wrap gap-2">
                {(isOperationsRateDashboard ? operationsRateViews : chartViews).map((item) => (
                  <MetricTabButton
                    key={item.key}
                    active={viewKey === item.key}
                    label={item.label}
                    description={item.description}
                    unitLabel={item.unitLabel}
                    onClick={() => setViewKey(item.key)}
                  />
                ))}
              </div>
            </div>

            {isLoadingRows ? (
              <LoadingState label="Loading time series" compact />
            ) : selectedSeries.length ? (
              <MetricLineChart months={months} series={selectedSeries} metricType={activeMetric.metricType} />
            ) : (
              <EmptyState
                title="No banks selected"
                description="Select at least one bank to render the line chart for the chosen metric."
                compact
              />
            )}
          </div>

          <div className="rounded-3xl border border-border bg-panel p-6">
            <div className="mb-4 flex items-center justify-between">
              <h3 className="text-lg font-semibold text-white">Selected banks</h3>
              <div className="rounded-full border border-border bg-panelMuted px-4 py-2 text-xs uppercase tracking-[0.2em] text-muted">
                {selectedBanks.length} selected
              </div>
            </div>

            <table className="min-w-full text-left text-sm">
              <thead>
                <tr className="border-b border-border text-muted">
                  <th className="pb-3 pr-4 font-medium">Bank</th>
                  <th className="pb-3 pr-4 font-medium">{activeMetric.label}</th>
                  {summaryRows.some((row) => row.share !== null) ? <th className="pb-3 font-medium">Share</th> : null}
                </tr>
              </thead>
              <tbody>
                {summaryRows.map((row) => (
                  <tr key={row.institutionCode} className="border-b border-border/60">
                    <td className="py-3 pr-4 text-white">{row.institutionName}</td>
                    <td className="py-3 pr-4 text-white">
                      {formatMetricValue(row.currentValue, activeMetric.metricType)}
                    </td>
                    {summaryRows.some((entry) => entry.share !== null) ? (
                      <td className="py-3 text-white">{row.share === null ? "—" : formatPercent(row.share)}</td>
                    ) : null}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <BankSelector
          banks={bankSeries.map((bank) => ({
            institutionCode: bank.institutionCode,
            institutionName: bank.institutionName,
          }))}
          selectedBanks={selectedBanks}
          onChange={setSelectedBanks}
        />
      </div>

      {errorMessage && (operationRows.length || operationsRateRows.length) ? (
        <ErrorState title="Partial data issue" description={errorMessage} compact />
      ) : null}
    </section>
  );
}

function getOperationMetricValue(
  row: CreditCardMetricRow,
  viewKey: ChartViewKey,
  activeUfValue: number
): number | null {
  if (viewKey === "volume") {
    return Number(row.real_value_uf) * activeUfValue;
  }
  if (viewKey === "transactions") {
    return Number(row.transaction_count);
  }
  if (viewKey === "average-ticket") {
    return Number(row.average_ticket_uf) * activeUfValue;
  }
  if (viewKey === "operations-per-active-card") {
    return row.operations_per_active_card === null ? null : Number(row.operations_per_active_card);
  }

  return null;
}

function getOperationsRateMetricValue(
  row: OperationsRateMetricRow,
  viewKey: OperationsRateViewKey
): number | null {
  if (viewKey === "total-active-cards") {
    return Number(row.total_active_cards);
  }
  if (viewKey === "total-cards-with-operations") {
    return Number(row.total_cards_with_operations);
  }
  if (viewKey === "operations-rate") {
    return row.operations_rate === null ? null : Number(row.operations_rate) * 100;
  }

  return null;
}

function formatMetricValue(value: number, metricType: MetricType): string {
  if (metricType === "money") {
    return formatMoney(value);
  }
  if (metricType === "ratio") {
    return formatPercent(value);
  }
  if (metricType === "decimal") {
    return formatDecimal(value);
  }
  return formatMoney(value);
}

function ControlCard({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="rounded-3xl border border-border bg-panel px-4 py-3">
      <p className="mb-2 text-xs font-semibold uppercase tracking-[0.2em] text-muted">{label}</p>
      {children}
    </div>
  );
}

function MetricTabButton({
  active,
  label,
  description,
  unitLabel,
  onClick,
}: {
  active: boolean;
  label: string;
  description: string;
  unitLabel: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "rounded-full border px-4 py-2 text-sm font-medium transition",
        active
          ? "border-brand/60 bg-brand/10 text-white"
          : "border-border bg-panelMuted text-muted hover:text-white"
      )}
    >
      <span className="inline-flex items-center gap-2">
        {label}
        <span className="group relative inline-flex h-5 w-5 items-center justify-center rounded-full border border-current/30 text-[10px] font-semibold leading-none text-current transition hover:border-brand/60 hover:text-white focus-visible:border-brand/60 focus-visible:text-white">
          i
          <span className="pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-72 -translate-x-1/2 rounded-2xl border border-border bg-[#07101c] p-3 text-left text-xs leading-5 text-muted shadow-2xl group-hover:block">
            <span className="block text-sm font-semibold text-white">{label}</span>
            <span className="mt-1 block">{description}</span>
            <span className="mt-1 block text-xs italic text-brand">{unitLabel}</span>
          </span>
        </span>
      </span>
    </button>
  );
}
