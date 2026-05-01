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
  formatMoneyWithSymbol,
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
type SummaryRow = {
  institutionCode: string;
  institutionName: string;
  currentValue: number | null;
  share: number | null;
  vsStart: number | null;
};

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

  const defaultSelectedBanks = useMemo(
    () => computeDefaultSelectedBanks(bankSeries, latestLoadedMonth),
    [bankSeries, latestLoadedMonth]
  );

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

      return defaultSelectedBanks;
    });
  }, [bankSeries, defaultSelectedBanks]);

  const selectedSeries = useMemo(
    () => bankSeries.filter((bank) => selectedBanks.includes(bank.institutionCode)),
    [bankSeries, selectedBanks]
  );

  const systemMonthTotals = useMemo(
    () =>
      Object.fromEntries(
        months.map((month) => [
          month,
          bankSeries.reduce((sum, bank) => {
            const value = bank.series[month];
            return typeof value === "number" ? sum + value : sum;
          }, 0),
        ])
      ) as Record<string, number>,
    [bankSeries, months]
  );

  const latestMonthRows = useMemo(
    () =>
      (isOperationsRateDashboard ? operationsRateRows : operationRows).filter(
        (row) => latestLoadedMonth !== null && row.period_month.slice(0, 7) === latestLoadedMonth
      ),
    [isOperationsRateDashboard, latestLoadedMonth, operationRows, operationsRateRows]
  );

  const firstMonthRows = useMemo(
    () =>
      (isOperationsRateDashboard ? operationsRateRows : operationRows).filter(
        (row) => row.period_month.slice(0, 7) === startMonth
      ),
    [isOperationsRateDashboard, operationRows, operationsRateRows, startMonth]
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

    const latestRowsByCode = new Map(latestMonthRows.map((row) => [row.institution_code, row] as const));
    const firstRowsByCode = new Map(firstMonthRows.map((row) => [row.institution_code, row] as const));

    const selectedRows: SummaryRow[] = selectedBanks
      .map((institutionCode) => {
        const row = latestRowsByCode.get(institutionCode);
        if (!row) {
          return null;
        }

        const currentValue = isOperationsRateDashboard
          ? getOperationsRateMetricValue(row as OperationsRateMetricRow, viewKey as OperationsRateViewKey)
          : getOperationMetricValue(row as CreditCardMetricRow, viewKey as ChartViewKey, activeUfValue);

        const startRow = firstRowsByCode.get(institutionCode);
        const startValue = startRow
          ? isOperationsRateDashboard
            ? getOperationsRateMetricValue(startRow as OperationsRateMetricRow, viewKey as OperationsRateViewKey)
            : getOperationMetricValue(startRow as CreditCardMetricRow, viewKey as ChartViewKey, activeUfValue)
          : null;

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
          vsStart: calculateVsStart(startValue, currentValue, startMonth === latestLoadedMonth),
        };
      })
      .filter((row): row is NonNullable<typeof row> => Boolean(row))
      .sort((left, right) => {
        if (left.currentValue === null && right.currentValue === null) {
          return 0;
        }
        if (left.currentValue === null) {
          return 1;
        }
        if (right.currentValue === null) {
          return -1;
        }
        return right.currentValue - left.currentValue;
      });

    if (isOperationsRateDashboard) {
      const systemCurrentValue = calculateOperationsRateSystemValue(
        latestMonthRows as OperationsRateMetricRow[],
        viewKey as OperationsRateViewKey
      );
      const systemStartValue = calculateOperationsRateSystemValue(
        firstMonthRows as OperationsRateMetricRow[],
        viewKey as OperationsRateViewKey
      );

      return systemCurrentValue === null
        ? selectedRows
        : [
            {
              institutionCode: "total",
              institutionName: "System",
              currentValue: systemCurrentValue,
              share: null,
              vsStart: calculateVsStart(systemStartValue, systemCurrentValue, startMonth === latestLoadedMonth),
            },
            ...selectedRows,
          ];
    }

    if (viewKey === "operations-per-active-card") {
      const systemCurrentValue = calculateOperationsPerActiveCardSystemValue(latestMonthRows as CreditCardMetricRow[]);
      const systemStartValue = calculateOperationsPerActiveCardSystemValue(firstMonthRows as CreditCardMetricRow[]);

      return systemCurrentValue === null
        ? selectedRows
        : [
            {
              institutionCode: "total",
              institutionName: "System",
              currentValue: systemCurrentValue,
              share: null,
              vsStart: calculateVsStart(systemStartValue, systemCurrentValue, startMonth === latestLoadedMonth),
            },
            ...selectedRows,
          ];
    }

    if (viewKey === "average-ticket") {
      const systemAvg =
        totals.transactions > 0 ? (totals.volume * 1_000_000) / totals.transactions : null;

      return systemAvg === null
        ? selectedRows
        : [
            {
              institutionCode: "total",
              institutionName: "System",
              currentValue: systemAvg,
              share: null,
              vsStart: calculateVsStart(
                calculateSystemAverage(firstMonthRows, activeUfValue),
                systemAvg,
                startMonth === latestLoadedMonth
              ),
            },
            ...selectedRows,
          ];
    }

    const selectedTotal = selectedRows.reduce((accumulator, row) => accumulator + (row.currentValue ?? 0), 0);
    const totalValue = viewKey === "volume" ? totals.volume : totals.transactions;
    const othersValue = Math.max(totalValue - selectedTotal, 0);
    const othersShare = calculateMarketShares(othersValue, totalValue);

    return [
      {
        institutionCode: "total",
        institutionName: "System",
        currentValue: totalValue,
        share: 100,
        vsStart: calculateVsStart(
          calculateSystemTotal(firstMonthRows, viewKey as ChartViewKey | OperationsRateViewKey, activeUfValue, isOperationsRateDashboard),
          totalValue,
          startMonth === latestLoadedMonth
        ),
      },
      ...selectedRows,
      {
        institutionCode: "others",
        institutionName: "Others",
        currentValue: othersValue,
        share: othersShare,
        vsStart: null,
      },
    ];
  }, [
    activeUfValue,
    firstMonthRows,
    isOperationsRateDashboard,
    latestLoadedMonth,
    latestMonthRows,
    selectedBanks,
    selectedSeries,
    startMonth,
    viewKey,
  ]);

  const shouldShowShareColumn = summaryRows.some((row) => row.share !== null);

  function handleResetBanks() {
    setSelectedBanks(defaultSelectedBanks);
    hasSeededSelectionRef.current = true;
  }

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
    <section className="space-y-10 pt-6 lg:pt-8">
      <div className="border-b border-border pb-8">
        <div className="max-w-4xl">
          <p className="text-xs font-semibold uppercase tracking-[0.28em] text-brand">Chilean banking · monthly series</p>
          <h1 className="mt-5 text-5xl font-semibold tracking-tight text-white sm:text-6xl">
            {getEditorialTitle(operationLabelMap[operation])}
          </h1>
          <p className="mt-5 max-w-none whitespace-nowrap text-base leading-7 text-muted sm:text-lg">
            Monthly credit-card analysis across banks, with UF-adjusted CLP values for volume and ticket metrics, plus the active-card and operations-rate views.
          </p>
        </div>
      </div>

      <div className="space-y-8">
        <div className="rounded-3xl border border-border bg-panel p-6">
          <div className="flex flex-col gap-5 xl:flex-row xl:items-end xl:justify-between">
            <div className={cn("grid gap-5", isOperationsRateDashboard ? "sm:grid-cols-2" : "sm:grid-cols-3")}>
              <ControlCard label="Start (MM/YY)">
                <select
                  value={startMonth}
                  onChange={(event) => setStartMonth(event.target.value)}
                  className="w-full border-0 border-b border-border bg-transparent px-0 py-2 text-sm text-white outline-none transition focus:border-brand/60"
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
                  className="w-full border-0 border-b border-border bg-transparent px-0 py-2 text-sm text-white outline-none transition focus:border-brand/60"
                >
                  {availableMonthOptions.map((month) => (
                    <option key={month} value={month} disabled={month < startMonth}>
                      {formatMonthLabel(month)}
                    </option>
                  ))}
                </select>
              </ControlCard>
              {!isOperationsRateDashboard ? (
                <ControlCard
                  label={
                    <span className="inline-flex items-center gap-2">
                      UF value
                      <span className="group relative inline-flex h-4 w-4 items-center justify-center rounded-full border border-current/30 text-[10px] font-semibold leading-none text-current cursor-help">
                        i
                        <span className="pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-56 -translate-x-1/2 rounded-2xl border border-border bg-[#07101c] p-3 text-left text-xs leading-5 text-muted shadow-2xl group-hover:block">
                          by default uses today&apos;s UF
                        </span>
                      </span>
                    </span>
                  }
                >
                  <div className="relative">
                    <span className="pointer-events-none absolute inset-y-0 left-0 flex items-center text-sm text-muted">$</span>
                    <input
                      value={ufInput}
                      onChange={(event) => {
                        const digits = event.target.value.replace(/\D/g, "");
                        setUfInput(digits ? formatMoney(Number(digits)) : "");
                      }}
                      inputMode="numeric"
                      className="w-full border-0 border-b border-border bg-transparent py-2 pl-5 pr-0 text-sm text-white outline-none transition focus:border-brand/60"
                    />
                  </div>
                </ControlCard>
              ) : null}
            </div>
          </div>
        </div>

        <div className="rounded-3xl border border-border bg-panel p-6">
          <div className="mb-6 flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div className="min-h-[4rem]">
              <h2 className="text-xl font-semibold text-white">{activeMetric.label}</h2>
              <p className="mt-2 min-h-5 text-xs italic text-muted">
                {activeMetric.key === "volume" ? "Millions of CLP" : activeMetric.unitLabel || "\u00A0"}
              </p>
            </div>

            <div className="flex flex-nowrap items-center gap-2 pb-1">
              {(isOperationsRateDashboard ? operationsRateViews : chartViews).map((item, index, items) => (
                <MetricTabButton
                  key={item.key}
                  active={viewKey === item.key}
                  tooltipAlign={index === items.length - 1 ? "right" : "center"}
                  label={item.label}
                  description={
                    !isOperationsRateDashboard
                      ? item.key === "average-ticket"
                        ? `Avg ${operationLabelMap[operation]} in CLP.`
                        : item.key === "operations-per-active-card"
                          ? `Number of ${operationLabelMap[operation]} per active credit card.`
                          : item.description.replace("for the selected operation", `for ${operationLabelMap[operation]}`)
                      : item.description
                  }
                  unitLabel={item.unitLabel}
                  onClick={() => setViewKey(item.key)}
                />
              ))}
            </div>
          </div>

          {isLoadingRows ? (
            <LoadingState label="Loading time series" compact />
          ) : selectedSeries.length ? (
            <MetricLineChart
              months={months}
              systemMonthTotals={systemMonthTotals}
              series={selectedSeries}
              metricType={activeMetric.metricType}
              showSystemShare={viewKey !== "transactions"}
            />
          ) : (
            <EmptyState
              title="No banks selected"
              description="Select at least one bank to render the line chart for the chosen metric."
              compact
            />
          )}
        </div>

        <BankSelector
          banks={bankSeries.map((bank) => ({
            institutionCode: bank.institutionCode,
            institutionName: bank.institutionName,
          }))}
          selectedBanks={selectedBanks}
          onChange={setSelectedBanks}
          onReset={handleResetBanks}
        />

        <div className="border-t border-border pt-8">
          <div className="mb-5 flex items-start justify-between gap-4">
            <div>
              <h3 className="text-3xl font-semibold tracking-tight text-white">
      {activeMetric.label} in {latestLoadedMonth ? formatMonthLabel(latestLoadedMonth) : formatMonthLabel(endMonth)}
    </h3>
  </div>
            <p className="text-sm text-muted">{selectedBanks.length} banks</p>
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-full text-left text-sm">
              <thead>
                <tr className="border-y border-border text-xs font-medium uppercase tracking-[0.24em] text-muted">
                  <th className="py-4 pr-6">Bank</th>
                  <th className="py-4 pr-6 text-right">{activeMetric.label}</th>
                  {shouldShowShareColumn ? <th className="py-4 pr-6 text-right">Share</th> : null}
                  <th className="py-4 text-right">v/s {formatMonthLabel(endMonth)}</th>
                </tr>
              </thead>
              <tbody>
                {summaryRows.map((row) => (
                  <tr
                    key={row.institutionCode}
                    className={cn("border-b border-border/70", row.institutionCode === "total" && "bg-brand/10")}
                  >
                    <td className={cn("py-5 pr-6 text-white", row.institutionCode === "total" ? "font-semibold" : "")}>
                      {row.institutionName}
                    </td>
                    <td className={cn("py-5 pr-6 text-right text-white", row.institutionCode === "total" ? "font-semibold" : "")}>
                      {row.currentValue === null ? "—" : formatMetricValue(row.currentValue, activeMetric.metricType)}
                    </td>
                    {shouldShowShareColumn ? (
                      <td className={cn("py-5 pr-6 text-right text-white", row.institutionCode === "total" ? "font-semibold" : "")}>
                        {row.share === null ? "—" : formatPercent(row.share)}
                      </td>
                    ) : null}
                    <td className={cn("py-5 text-right text-white", row.institutionCode === "total" ? "font-semibold" : "")}>
                      {row.vsStart === null ? "—" : formatPercent(row.vsStart)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
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
  if (viewKey === "supplementary-rate") {
    const primaryCards = Number(row.active_cards_primary);
    return primaryCards > 0 ? (Number(row.active_cards_supplementary) / primaryCards) * 100 : null;
  }

  return null;
}

function formatMetricValue(value: number, metricType: MetricType): string {
  if (metricType === "money") {
    return formatMoneyWithSymbol(value);
  }
  if (metricType === "ratio") {
    return formatPercent(value);
  }
  if (metricType === "decimal") {
    return formatDecimal(value);
  }
  return formatMoney(value);
}

function ControlCard({ label, children }: { label: React.ReactNode; children: React.ReactNode }) {
  return (
    <div className="min-w-0">
      <p className="mb-1 text-[10px] font-semibold uppercase tracking-[0.24em] text-muted">{label}</p>
      {children}
    </div>
  );
}

function MetricTabButton({
  active,
  tooltipAlign,
  label,
  description,
  unitLabel,
  onClick,
}: {
  active: boolean;
  tooltipAlign?: "center" | "right";
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
        "group/tab relative shrink-0 whitespace-nowrap rounded-full border px-3 py-1.5 text-xs font-medium transition",
        active
          ? "border-brand/60 bg-brand/10 text-white"
          : "border-border bg-panelMuted text-muted hover:text-white"
      )}
    >
      <span className="inline-flex items-center gap-2">
        {label}
        <span className="group/info relative inline-flex cursor-help">
          <span className="inline-flex h-4 w-4 items-center justify-center rounded-full border border-current/30 text-[10px] font-semibold leading-none text-current transition group-hover/tab:border-brand/60 group-hover/tab:text-white group-focus-visible/tab:border-brand/60 group-focus-visible/tab:text-white">
            i
          </span>
          <span
            className={cn(
              "pointer-events-none absolute bottom-full z-30 mb-2 hidden w-80 max-w-[90vw] whitespace-normal break-words rounded-2xl border border-border bg-[#07101c] p-3 text-left text-xs leading-5 text-muted shadow-2xl group-hover/info:block group-focus-visible/info:block",
              tooltipAlign === "right" ? "right-0" : "left-1/2 -translate-x-1/2"
            )}
          >
            <span className="block text-sm font-semibold text-white">{label}</span>
            <span className="mt-1 block">{description}</span>
            {unitLabel ? <span className="mt-1 block text-xs italic text-brand">{unitLabel}</span> : null}
          </span>
        </span>
      </span>
    </button>
  );
}

function computeDefaultSelectedBanks(
  bankSeries: Array<{
    institutionCode: string;
    series: Record<string, number | null>;
  }>,
  latestLoadedMonth: string | null
) {
  if (!latestLoadedMonth) {
    return [];
  }

  return bankSeries
    .map((bank) => ({
      institutionCode: bank.institutionCode,
      value: bank.series[latestLoadedMonth],
    }))
    .filter((bank): bank is { institutionCode: string; value: number } => typeof bank.value === "number")
    .sort((left, right) => right.value - left.value)
    .slice(0, 5)
    .map((item) => item.institutionCode);
}

function calculateVsStart(startValue: number | null, currentValue: number | null, sameMonth: boolean) {
  if (sameMonth || startValue === null || startValue === 0 || currentValue === null) {
    return null;
  }

  return ((currentValue - startValue) / startValue) * 100;
}

function calculateSystemAverage(rows: Array<CreditCardMetricRow | OperationsRateMetricRow>, activeUfValue: number) {
  const totals = rows.reduce(
    (accumulator, row) => {
      if (!("real_value_uf" in row) || !("transaction_count" in row)) {
        return accumulator;
      }

      accumulator.volume += Number(row.real_value_uf) * activeUfValue;
      accumulator.transactions += Number(row.transaction_count);
      return accumulator;
    },
    { volume: 0, transactions: 0 }
  );

  return totals.transactions > 0 ? (totals.volume * 1_000_000) / totals.transactions : null;
}

function calculateOperationsPerActiveCardSystemValue(rows: CreditCardMetricRow[]) {
  const totals = rows.reduce(
    (accumulator, row) => {
      accumulator.transactions += Number(row.transaction_count);
      accumulator.activeCards += Number(row.total_active_cards ?? 0);
      return accumulator;
    },
    { transactions: 0, activeCards: 0 }
  );

  return totals.activeCards > 0 ? totals.transactions / totals.activeCards : null;
}

function calculateOperationsRateSystemValue(rows: OperationsRateMetricRow[], viewKey: OperationsRateViewKey) {
  const totals = rows.reduce(
    (accumulator, row) => {
      accumulator.totalActiveCards += Number(row.total_active_cards);
      accumulator.activeCardsPrimary += Number(row.active_cards_primary);
      accumulator.activeCardsSupplementary += Number(row.active_cards_supplementary);
      accumulator.totalCardsWithOperations += Number(row.total_cards_with_operations);
      return accumulator;
    },
    {
      totalActiveCards: 0,
      activeCardsPrimary: 0,
      activeCardsSupplementary: 0,
      totalCardsWithOperations: 0,
    }
  );

  if (viewKey === "total-active-cards") {
    return totals.totalActiveCards;
  }
  if (viewKey === "total-cards-with-operations") {
    return totals.totalCardsWithOperations;
  }
  if (viewKey === "operations-rate") {
    return totals.totalActiveCards > 0 ? (totals.totalCardsWithOperations / totals.totalActiveCards) * 100 : null;
  }
  if (viewKey === "supplementary-rate") {
    return totals.activeCardsPrimary > 0
      ? (totals.activeCardsSupplementary / totals.activeCardsPrimary) * 100
      : null;
  }

  return null;
}

function calculateSystemTotal(
  rows: Array<CreditCardMetricRow | OperationsRateMetricRow>,
  viewKey: ChartViewKey | OperationsRateViewKey,
  activeUfValue: number,
  isOperationsRateDashboard: boolean
) {
  return rows.reduce((total, row) => {
    const nextValue = isOperationsRateDashboard
      ? getOperationsRateMetricValue(row as OperationsRateMetricRow, viewKey as OperationsRateViewKey)
      : getOperationMetricValue(row as CreditCardMetricRow, viewKey as ChartViewKey, activeUfValue);

    return nextValue === null ? total : total + nextValue;
  }, 0);
}

function getEditorialTitle(operationLabel: string) {
  if (operationLabel === "Operations Rate") {
    return "Credit-card operations rate across the system";
  }

  return `${operationLabel} bank by bank`;
}
