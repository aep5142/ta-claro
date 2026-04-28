"use client";

import { useEffect, useMemo, useState } from "react";
import { BankSelector } from "@/components/bank-selector";
import { EmptyState, ErrorState, LoadingState } from "@/components/dashboard-states";
import { MetricLineChart } from "@/components/metric-line-chart";
import {
  type CreditCardMetricRow,
  fetchCreditCardMetrics,
  fetchDatasetBoundary,
  fetchLatestUfValue,
} from "@/lib/supabase-queries";
import {
  chartViewByKey,
  chartViews,
  defaultViewKey,
  isChartViewKey,
  operationLabelMap,
  type ChartViewKey,
  type OperationName,
} from "@/lib/credit-card-config";
import {
  addMonths,
  buildMonthOptions,
  calculateMarketShares,
  formatMonthLabel,
  formatMoney,
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
  defaultUfValue: number;
  defaultUfDate: string;
};

export function CreditCardsDashboard({
  operation,
  operationSlug,
  initialView,
}: CreditCardsDashboardProps) {
  const initialViewKey = isChartViewKey(initialView) ? initialView : defaultViewKey;

  const [viewKey, setViewKey] = useState<ChartViewKey>(initialViewKey);
  const [boundaryState, setBoundaryState] = useState<BoundaryState | null>(null);
  const [startMonth, setStartMonth] = useState("");
  const [endMonth, setEndMonth] = useState("");
  const [rows, setRows] = useState<CreditCardMetricRow[]>([]);
  const [selectedBanks, setSelectedBanks] = useState<string[]>([]);
  const [ufInput, setUfInput] = useState("");
  const [isLoadingBounds, setIsLoadingBounds] = useState(true);
  const [isLoadingRows, setIsLoadingRows] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    setViewKey(isChartViewKey(initialView) ? initialView : defaultViewKey);
  }, [initialView]);

  useEffect(() => {
    let isCancelled = false;

    async function loadBoundaries() {
      setIsLoadingBounds(true);
      setErrorMessage(null);
      setRows([]);

      try {
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
          addMonths(parseMonthValue(latestMonth), -11)
            .toISOString()
            .slice(0, 7)
        );
        const safeStart = boundedStart < earliestMonth.slice(0, 7) ? earliestMonth.slice(0, 7) : boundedStart;

        if (!isCancelled) {
          setBoundaryState({
            earliestMonth: earliestMonth.slice(0, 7),
            latestMonth: latestMonth.slice(0, 7),
            defaultUfDate: latestUf.ufDate,
            defaultUfValue: latestUf.value,
          });
          setStartMonth(safeStart);
          setEndMonth(latestMonth.slice(0, 7));
          setUfInput(String(Math.round(latestUf.value)));
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
  }, [operation]);

  useEffect(() => {
    if (!startMonth || !endMonth) {
      return;
    }

    let isCancelled = false;

    async function loadRows() {
      setIsLoadingRows(true);
      setErrorMessage(null);

      try {
        const nextRows = await fetchCreditCardMetrics(operation, `${startMonth}-01`, `${endMonth}-01`);

        if (!nextRows.length) {
          throw new Error("The selected time range returned no rows.");
        }

        if (!isCancelled) {
          setRows(nextRows);
        }
      } catch (error) {
        if (!isCancelled) {
          setRows([]);
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
  }, [operation, startMonth, endMonth]);

  const activeUfValue = useMemo(() => {
    const parsed = Number(ufInput.replace(/\./g, "").replace(",", "."));
    return Number.isFinite(parsed) && parsed > 0 ? parsed : boundaryState?.defaultUfValue ?? 0;
  }, [boundaryState?.defaultUfValue, ufInput]);

  const months = useMemo(() => buildMonthOptions(startMonth, endMonth), [startMonth, endMonth]);

  const activeMetric = chartViews.find((item) => item.key === viewKey) ?? chartViews[0];
  const loadedMonthKeys = useMemo(
    () => Array.from(new Set(rows.map((row) => row.period_month.slice(0, 7)))).sort(),
    [rows]
  );
  const latestLoadedMonth = loadedMonthKeys.at(-1) ?? null;

  const bankSeries = useMemo(() => {
    const grouped = new Map<string, Record<string, number | null>>();
    const bankNames = new Map<string, string>();

    rows.forEach((row) => {
      const monthKey = row.period_month.slice(0, 7);
      const metricValue =
        viewKey === "volume"
          ? Number(row.real_value_uf) * activeUfValue
          : viewKey === "transactions"
            ? Number(row.transaction_count)
            : Number(row.average_ticket_uf) * activeUfValue;

      const bankMonths = grouped.get(row.institution_code) ?? Object.fromEntries(
        months.map((month) => [month, null])
      ) as Record<string, number | null>;
      bankMonths[monthKey] = metricValue;
      grouped.set(row.institution_code, bankMonths);
      bankNames.set(row.institution_code, row.institution_name);
    });

    return Array.from(grouped.entries())
      .map(([institutionCode, series]) => ({
        institutionCode,
        institutionName: bankNames.get(institutionCode) ?? institutionCode,
        series,
      }))
      .sort((left, right) => left.institutionName.localeCompare(right.institutionName));
  }, [activeUfValue, months, rows, viewKey]);

  const latestVisibleMonth = latestLoadedMonth;

  useEffect(() => {
    if (!bankSeries.length || !latestVisibleMonth) {
      setSelectedBanks([]);
      return;
    }

    const ranked = bankSeries
      .map((bank) => ({
        institutionCode: bank.institutionCode,
        value: bank.series[latestVisibleMonth],
      }))
      .filter((bank): bank is { institutionCode: string; value: number } => typeof bank.value === "number")
      .sort((left, right) => right.value - left.value)
      .slice(0, 5)
      .map((item) => item.institutionCode);

    setSelectedBanks(ranked);
  }, [bankSeries, latestVisibleMonth, operation, viewKey, startMonth, endMonth]);

  const selectedSeries = useMemo(
    () => bankSeries.filter((bank) => selectedBanks.includes(bank.institutionCode)),
    [bankSeries, selectedBanks]
  );

  const latestMonthRows = useMemo(
    () => rows.filter((row) => latestVisibleMonth !== null && row.period_month.slice(0, 7) === latestVisibleMonth),
    [latestVisibleMonth, rows]
  );

  const summaryRows = useMemo(() => {
    const bankMap = new Map(
      selectedSeries.map((bank) => [bank.institutionCode, bank.institutionName] as const)
    );

    const totals = latestMonthRows.reduce(
      (accumulator, row) => {
        accumulator.volume += Number(row.real_value_uf) * activeUfValue;
        accumulator.transactions += Number(row.transaction_count);
        return accumulator;
      },
      { volume: 0, transactions: 0 }
    );

    return selectedBanks
      .map((institutionCode) => {
        const row = latestMonthRows.find((item) => item.institution_code === institutionCode);
        if (!row) {
          return null;
        }

        const volumeValue = Number(row.real_value_uf) * activeUfValue;
        const transactionsValue = Number(row.transaction_count);
        const averageTicketValue = Number(row.average_ticket_uf) * activeUfValue;
        const currentValue =
          viewKey === "volume"
            ? volumeValue
            : viewKey === "transactions"
              ? transactionsValue
              : averageTicketValue;

        const share =
          viewKey === "average-ticket"
            ? null
            : calculateMarketShares(
                viewKey === "volume" ? volumeValue : transactionsValue,
                viewKey === "volume" ? totals.volume : totals.transactions
              );

        return {
          institutionCode,
          institutionName: bankMap.get(institutionCode) ?? row.institution_name,
          currentValue,
          share,
        };
      })
      .filter((row): row is NonNullable<typeof row> => Boolean(row))
      .sort((left, right) => right.currentValue - left.currentValue);
  }, [activeUfValue, latestMonthRows, selectedBanks, selectedSeries, viewKey]);

  const availableMonthOptions = useMemo(() => {
    if (!boundaryState) {
      return [];
    }

    return buildMonthOptions(boundaryState.earliestMonth, boundaryState.latestMonth);
  }, [boundaryState]);

  if (isLoadingBounds) {
    return <LoadingState label="Loading dashboard configuration" />;
  }

  if (errorMessage && !rows.length) {
    return <ErrorState title="Unable to load the dashboard" description={errorMessage} />;
  }

  if (!boundaryState) {
    return <EmptyState title="No data available" description="No operation metadata was returned." />;
  }

  if (!latestVisibleMonth && !isLoadingRows) {
    return <EmptyState title="No loaded data" description="The selected time range did not return any complete points." />;
  }

  return (
    <section className="space-y-6">
      <div className="rounded-3xl border border-border bg-panel p-6">
        <div className="flex flex-col gap-6 xl:flex-row xl:items-end xl:justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-brand">Credit Cards</p>
            <h1 className="mt-3 text-3xl font-semibold text-white">{operationLabelMap[operation]}</h1>
            <p className="mt-3 max-w-2xl text-sm leading-6 text-muted">
              Shareable route: <span className="text-white">/credit-cards/{operationSlug}</span>. Data comes from the
              public unified operations view and uses UF-adjusted CLP where applicable.
            </p>
          </div>

          <div className="grid gap-3 sm:grid-cols-3">
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
            <ControlCard label={`UF (${boundaryState.defaultUfDate})`}>
              <input
                value={ufInput}
                onChange={(event) => setUfInput(event.target.value)}
                inputMode="decimal"
                className="w-full rounded-2xl border border-border bg-panelMuted px-3 py-2 text-sm text-white outline-none transition focus:border-brand/60"
              />
            </ControlCard>
          </div>
        </div>
      </div>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_320px]">
        <div className="space-y-6">
          <div className="rounded-3xl border border-border bg-panel p-6">
            <div className="mb-6 flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
              <div>
                <h2 className="text-xl font-semibold text-white">{activeMetric.label}</h2>
                <p className="mt-2 text-sm text-muted">
                  {formatMonthLabel(startMonth)} to {formatMonthLabel(endMonth)} · latest loaded month{" "}
                  {latestVisibleMonth ? formatMonthLabel(latestVisibleMonth) : "—"}
                </p>
              </div>

              <div className="flex flex-wrap gap-2">
                {chartViews.map((item) => (
                  <button
                    key={item.key}
                    type="button"
                    onClick={() => setViewKey(item.key)}
                    className={cn(
                      "rounded-full border px-4 py-2 text-sm font-medium transition",
                      viewKey === item.key
                        ? "border-brand/60 bg-brand/10 text-white"
                        : "border-border bg-panelMuted text-muted hover:text-white"
                    )}
                  >
                    {item.label}
                  </button>
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
              <div>
                <h3 className="text-lg font-semibold text-white">Latest visible month table</h3>
                <p className="mt-1 text-sm text-muted">{latestVisibleMonth ? formatMonthLabel(latestVisibleMonth) : "—"}</p>
              </div>
              <div className="rounded-full border border-border bg-panelMuted px-4 py-2 text-xs uppercase tracking-[0.2em] text-muted">
                {selectedBanks.length} selected
              </div>
            </div>

            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-sm">
                <thead>
                  <tr className="border-b border-border text-muted">
                    <th className="pb-3 pr-4 font-medium">Bank</th>
                    <th className="pb-3 pr-4 font-medium">{activeMetric.label}</th>
                    <th className="pb-3 font-medium">Share</th>
                  </tr>
                </thead>
                <tbody>
                  {summaryRows.map((row) => (
                    <tr key={row.institutionCode} className="border-b border-border/60">
                      <td className="py-3 pr-4 text-white">{row.institutionName}</td>
                      <td className="py-3 pr-4 text-white">{formatMoney(row.currentValue)}</td>
                      <td className="py-3 text-white">{row.share === null ? "—" : formatPercent(row.share)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <BankSelector
          banks={bankSeries.map((bank) => ({
            institutionCode: bank.institutionCode,
            institutionName: bank.institutionName,
            latestValue: latestVisibleMonth ? bank.series[latestVisibleMonth] : null,
          }))}
          metricLabel={activeMetric.label}
          selectedBanks={selectedBanks}
          onChange={setSelectedBanks}
        />
      </div>

      {errorMessage && rows.length ? <ErrorState title="Partial data issue" description={errorMessage} compact /> : null}
    </section>
  );
}

function ControlCard({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="rounded-3xl border border-border bg-panel px-4 py-3">
      <p className="mb-2 text-xs font-semibold uppercase tracking-[0.2em] text-muted">{label}</p>
      {children}
    </div>
  );
}
