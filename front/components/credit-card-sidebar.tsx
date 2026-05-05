"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import {
  creditCardOperations,
  defaultOperationsRateViewKey,
  defaultViewKey,
  isOperationsRateOperation,
  type OperationName,
  operationFromSlug,
} from "@/lib/credit-card-config";
import {
  debitCardOperations,
  defaultDebitOperationsRateViewKey,
  defaultDebitViewKey,
  isDebitChartViewKey,
  isDebitOperationsRateOperation,
  isDebitOperationsRateViewKey,
  operationFromSlug as debitOperationFromSlug,
  type DebitOperationName,
} from "@/lib/debit-card-config";
import { formatMoney, formatMonthLabel, getChileTodayIso, normalizeMonthValue } from "@/lib/formatters";
import { fetchDatasetBoundary, fetchLatestUfValue, fetchOperationsRateBoundary } from "@/lib/supabase-queries";
import {
  fetchDebitDatasetBoundary,
  fetchDebitOperationMetricsBoundary,
} from "@/lib/supabase-debit-queries";
import { cn } from "@/lib/utils";

type CreditCardSidebarProps = {
  section: "credit-cards" | "debit-cards" | "accounts" | "loans";
  activeOperation?: string;
  queryParams?: Record<string, string | undefined>;
  onNavigate?: () => void;
};

type BoundaryState = {
  earliestMonth: string;
  latestMonth: string;
  defaultUfValue: number | null;
};

function addMonths(month: string, offset: number): string {
  const [year, m] = month.split("-").map(Number);
  const date = new Date(Date.UTC(year, m - 1 + offset, 1));
  return date.toISOString().slice(0, 7);
}

function monthRegex(month: string | null): month is string {
  return Boolean(month && /^\d{4}-\d{2}$/.test(month));
}

function isDatasetOperation(
  operation: OperationName
): operation is Exclude<OperationName, "Total Activation Rate"> {
  return operation !== "Total Activation Rate";
}

function isDebitDatasetOperation(
  operation: DebitOperationName
): operation is Exclude<DebitOperationName, "Total Activation Rate"> {
  return operation !== "Total Activation Rate";
}

export function CreditCardSidebar({
  section,
  activeOperation,
  queryParams = {},
  onNavigate,
}: CreditCardSidebarProps) {
  const sectionTitle =
    section === "credit-cards"
      ? "Credit Cards"
      : section === "debit-cards"
        ? "Debit Cards"
        : section === "accounts"
          ? "Accounts"
          : "Loans";

  const router = useRouter();
  const pathname = usePathname();
  const [boundaryState, setBoundaryState] = useState<BoundaryState | null>(null);
  const searchParams = useMemo(() => {
    const params = new URLSearchParams();
    Object.entries(queryParams).forEach(([key, value]) => {
      if (value) {
        params.set(key, value);
      }
    });
    return params;
  }, [queryParams]);

  const operationName = useMemo<OperationName | DebitOperationName | null>(() => {
    if (!activeOperation) {
      return null;
    }
    if (section === "credit-cards") {
      return operationFromSlug(activeOperation);
    }
    if (section === "debit-cards") {
      return debitOperationFromSlug(activeOperation);
    }
    return null;
  }, [activeOperation, section]);

  const isOperationsRateDashboard =
    operationName === null
      ? false
      : section === "credit-cards"
        ? isOperationsRateOperation(operationName as OperationName)
        : section === "debit-cards"
          ? isDebitOperationsRateOperation(operationName as DebitOperationName)
          : false;

  useEffect(() => {
    let isCancelled = false;

    async function loadBoundaries() {
      if ((section !== "credit-cards" && section !== "debit-cards") || !operationName) {
        setBoundaryState(null);
        return;
      }

      try {
        if (isOperationsRateDashboard) {
          const [latestMonth, earliestMonth] =
            section === "credit-cards"
              ? await Promise.all([
                  fetchOperationsRateBoundary("latest"),
                  fetchOperationsRateBoundary("earliest"),
                ])
              : await Promise.all([
                  fetchDebitOperationMetricsBoundary("latest"),
                  fetchDebitOperationMetricsBoundary("earliest"),
                ]);

          if (!latestMonth || !earliestMonth) {
            throw new Error("No activation-rate data is available.");
          }

          if (!isCancelled) {
            setBoundaryState({
              earliestMonth: earliestMonth.slice(0, 7),
              latestMonth: latestMonth.slice(0, 7),
              defaultUfValue: null,
            });
          }
          return;
        }

        if (section === "credit-cards" && !isDatasetOperation(operationName as OperationName)) {
          return;
        }
        if (section === "debit-cards" && !isDebitDatasetOperation(operationName as DebitOperationName)) {
          return;
        }

        const chileToday = getChileTodayIso();
        const [latestMonth, earliestMonth, latestUf] =
          section === "credit-cards"
            ? await Promise.all([
                fetchDatasetBoundary(
                  operationName as Exclude<OperationName, "Total Activation Rate">,
                  "latest"
                ),
                fetchDatasetBoundary(
                  operationName as Exclude<OperationName, "Total Activation Rate">,
                  "earliest"
                ),
                fetchLatestUfValue(chileToday),
              ])
            : await Promise.all([
                fetchDebitDatasetBoundary(operationName as Exclude<DebitOperationName, "Total Activation Rate">, "latest"),
                fetchDebitDatasetBoundary(operationName as Exclude<DebitOperationName, "Total Activation Rate">, "earliest"),
                fetchLatestUfValue(chileToday),
              ]);

        if (!latestMonth || !earliestMonth) {
          throw new Error(
            section === "credit-cards"
              ? "No credit-card data is available for this operation."
              : "No debit-card data is available for this operation."
          );
        }

        if (!isCancelled) {
          setBoundaryState({
            earliestMonth: earliestMonth.slice(0, 7),
            latestMonth: latestMonth.slice(0, 7),
            defaultUfValue: latestUf.value,
          });
        }
      } catch {
        if (!isCancelled) {
          setBoundaryState(null);
        }
      }
    }

    void loadBoundaries();

    return () => {
      isCancelled = true;
    };
  }, [isOperationsRateDashboard, operationName, section]);

  const persistedParams = useMemo(() => {
    const params = new URLSearchParams(searchParams.toString());

    if (boundaryState) {
      const earliest = boundaryState.earliestMonth;
      const latest = boundaryState.latestMonth;
      const defaultStart = addMonths(latest, -12) < earliest ? earliest : addMonths(latest, -12);

      const startParam = searchParams.get("start");
      const endParam = searchParams.get("end");

      const baseStart = monthRegex(startParam) ? normalizeMonthValue(startParam) : defaultStart;
      const baseEnd = monthRegex(endParam) ? normalizeMonthValue(endParam) : latest;

      const safeStart = baseStart < earliest ? earliest : baseStart > latest ? latest : baseStart;
      const safeEnd = baseEnd < earliest ? earliest : baseEnd > latest ? latest : baseEnd;

      const normalizedStart = safeStart > safeEnd ? safeEnd : safeStart;
      const normalizedEnd = safeEnd < normalizedStart ? normalizedStart : safeEnd;

      params.set("start", normalizedStart);
      params.set("end", normalizedEnd);

      const ufParam = searchParams.get("uf");
      const ufNumber = ufParam ? Number(ufParam) : NaN;
      const fallbackUf = boundaryState.defaultUfValue ? String(Math.round(boundaryState.defaultUfValue)) : "";

      if (Number.isFinite(ufNumber) && ufNumber > 0) {
        params.set("uf", String(Math.round(ufNumber)));
      } else if (fallbackUf) {
        params.set("uf", fallbackUf);
      }
    }

    if (!params.get("view")) {
      if (section === "credit-cards") {
        params.set("view", isOperationsRateDashboard ? defaultOperationsRateViewKey : defaultViewKey);
      } else if (section === "debit-cards") {
        params.set("view", isOperationsRateDashboard ? defaultDebitOperationsRateViewKey : defaultDebitViewKey);
      }
    }

    return params;
  }, [boundaryState, isOperationsRateDashboard, searchParams, section]);

  useEffect(() => {
    if ((section !== "credit-cards" && section !== "debit-cards") || !boundaryState) {
      return;
    }

    if (persistedParams.toString() !== searchParams.toString()) {
      router.replace(`${pathname}?${persistedParams.toString()}`, { scroll: false });
    }
  }, [boundaryState, pathname, persistedParams, router, searchParams, section]);

  const availableMonthOptions = useMemo(() => {
    if (!boundaryState) {
      return [];
    }

    const months: string[] = [];
    let cursor = boundaryState.earliestMonth;

    while (cursor <= boundaryState.latestMonth) {
      months.push(cursor);
      cursor = addMonths(cursor, 1);
    }

    return months;
  }, [boundaryState]);

  const updateParam = (key: string, value: string) => {
    const params = new URLSearchParams(persistedParams.toString());
    params.set(key, value);
    router.replace(`${pathname}?${params.toString()}`, { scroll: false });
  };

  const startMonth = persistedParams.get("start") ?? "";
  const endMonth = persistedParams.get("end") ?? "";
  const ufValue = persistedParams.get("uf") ?? "";

  const operationHref = (slug: string) => {
    const params = new URLSearchParams(persistedParams.toString());
    if (!params.get("view")) {
      if (section === "credit-cards") {
        params.set("view", slug === "total-activation-rate" ? defaultOperationsRateViewKey : defaultViewKey);
      } else if (section === "debit-cards") {
        params.set("view", slug === "total-activation-rate" ? defaultDebitOperationsRateViewKey : defaultDebitViewKey);
      }
    }
    const basePath = section === "debit-cards" ? "/debit-cards" : "/credit-cards";
    return `${basePath}/${slug}?${params.toString()}`;
  };

  return (
    <div className="py-2">
      <div className="mb-8">
        <h2 className="text-xs font-semibold uppercase tracking-[0.28em] text-slate-500">{sectionTitle}</h2>
      </div>

      {section === "credit-cards" || section === "debit-cards" ? (
        <div className="space-y-6">
          <div className="space-y-4">
            {(section === "credit-cards" ? creditCardOperations : debitCardOperations).map((item) => {
              const isActive = activeOperation === item.slug;

              return (
                <Link
                  key={item.slug}
                  href={operationHref(item.slug)}
                  onClick={onNavigate}
                  className={cn(
                    "block border-l-2 pl-4 text-[15px] transition",
                    isActive
                      ? "border-brand font-semibold text-slate-950"
                      : "border-transparent text-slate-700 hover:text-slate-950"
                  )}
                >
                  <span>{item.label}</span>
                </Link>
              );
            })}
          </div>

          <div className="border-t border-dashed border-slate-300 pt-4">
            <h3 className="mb-3 text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Inputs</h3>

            <div className="space-y-3">
              <div className="pb-3 border-b border-dashed border-slate-300">
                <label className="mb-1 block text-[11px] font-semibold uppercase tracking-[0.2em] text-slate-600">Start (MM/YY)</label>
                <select
                  value={startMonth}
                  onChange={(event) => updateParam("start", event.target.value)}
                  className="w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-sm text-slate-900"
                >
                  {availableMonthOptions.map((month) => (
                    <option key={month} value={month} disabled={month > endMonth}>
                      {formatMonthLabel(month)}
                    </option>
                  ))}
                </select>
              </div>

              <div className="pb-3 border-b border-dashed border-slate-300">
                <label className="mb-1 block text-[11px] font-semibold uppercase tracking-[0.2em] text-slate-600">End (MM/YY)</label>
                <select
                  value={endMonth}
                  onChange={(event) => updateParam("end", event.target.value)}
                  className="w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-sm text-slate-900"
                >
                  {availableMonthOptions.map((month) => (
                    <option key={month} value={month} disabled={month < startMonth}>
                      {formatMonthLabel(month)}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label className="mb-1 block text-[11px] font-semibold uppercase tracking-[0.2em] text-slate-600">
                  <span className="inline-flex items-center gap-1.5">
                    UF value
                    <span className="group relative inline-flex h-4 w-4 items-center justify-center rounded-full border border-current/30 text-[10px] font-semibold leading-none text-current cursor-help">
                      i
                      <span className="pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-56 -translate-x-1/2 rounded-2xl border border-slate-300 bg-white p-3 text-left text-xs leading-5 text-slate-700 shadow-2xl group-hover:block">
                        by default uses today&apos;s UF
                      </span>
                    </span>
                  </span>
                </label>
                <div className="relative">
                  <span className="pointer-events-none absolute inset-y-0 left-2 flex items-center text-sm text-slate-600">$</span>
                  <input
                    value={formatMoney(Number(ufValue || "0"))}
                    onChange={(event) => {
                      const digits = event.target.value.replace(/\D/g, "");
                      if (!digits) {
                        return;
                      }
                      updateParam("uf", String(Number(digits)));
                    }}
                    inputMode="numeric"
                    className="w-full rounded border border-slate-300 bg-white py-1.5 pl-6 pr-2 text-sm text-slate-900"
                  />
                </div>
              </div>
            </div>
          </div>
        </div>
      ) : (
        <div className="max-w-[18rem] text-sm leading-6 text-slate-700">
          Only the Credit Cards section is connected in v1. This route remains part of the shared shell.
        </div>
      )}
    </div>
  );
}
