"use client";

import { useState } from "react";
import Link from "next/link";
import { CreditCardSidebar } from "@/components/credit-card-sidebar";
import { primarySections } from "@/lib/credit-card-config";
import { cn } from "@/lib/utils";

type AppShellProps = {
  children: React.ReactNode;
  section: "credit-cards" | "debit-cards" | "accounts" | "loans";
  activeOperation?: string;
  queryParams?: Record<string, string | undefined>;
};

export function AppShell({ children, section, activeOperation, queryParams = {} }: AppShellProps) {
  const [isMobileSidebarOpen, setIsMobileSidebarOpen] = useState(false);

  const mergedHref = (href: string) => {
    const [basePath, query = ""] = href.split("?");
    const params = new URLSearchParams(query);
    Object.entries(queryParams).forEach(([key, value]) => {
      if (value && !params.has(key)) {
        params.set(key, value);
      }
    });
    const mergedQuery = params.toString();
    return mergedQuery ? `${basePath}?${mergedQuery}` : basePath;
  };

  return (
    <div className="min-h-screen bg-surface">
      <header className="sticky top-0 z-50 bg-white text-slate-950 shadow-sm">
        <div className="flex h-16 w-full items-center justify-between gap-3 px-4 sm:px-6 lg:hidden">
          <Link href={mergedHref("/credit-cards/purchases?view=volume")} className="min-w-0">
            <p className="truncate text-lg font-semibold tracking-tight text-slate-950">Ta-Claro</p>
          </Link>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => setIsMobileSidebarOpen(true)}
              className="rounded border border-slate-300 px-3 py-1.5 text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-700"
            >
              Menu
            </button>
            <button
              type="button"
              className="rounded-sm border border-slate-950 bg-slate-950 px-3 py-1.5 text-[10px] font-semibold uppercase tracking-[0.18em] text-white transition hover:bg-brand hover:text-white"
            >
              Login
            </button>
          </div>
        </div>

        <div className="border-t border-slate-200 px-4 py-2 sm:px-6 lg:hidden">
          <nav className="flex items-center gap-5 overflow-x-auto whitespace-nowrap pb-0.5">
            {primarySections.map((item) => {
              const isActive = section === item.slug;

              return (
                <Link
                  key={item.slug}
                  href={mergedHref(item.href)}
                  className={cn(
                    "border-b-2 pb-0.5 text-[10px] font-medium uppercase tracking-[0.18em] transition",
                    isActive ? "border-brand text-slate-950" : "border-transparent text-slate-500 hover:text-slate-950"
                  )}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>
        </div>

        <div className="hidden h-16 w-full grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-4 px-4 sm:px-6 lg:grid lg:px-8">
          <Link href={mergedHref("/credit-cards/purchases?view=volume")} className="justify-self-start">
            <p className="text-xl font-semibold tracking-tight text-slate-950">Ta-Claro</p>
          </Link>

          <nav className="flex flex-wrap items-center justify-center gap-7">
            {primarySections.map((item) => {
              const isActive = section === item.slug;

              return (
                <Link
                  key={item.slug}
                  href={mergedHref(item.href)}
                  className={cn(
                    "border-b-2 pb-0.5 text-[11px] font-medium uppercase tracking-[0.28em] transition",
                    isActive ? "border-brand text-slate-950" : "border-transparent text-slate-500 hover:text-slate-950"
                  )}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>

          <div className="justify-self-end">
            <button
              type="button"
              className="rounded-sm border border-slate-950 bg-slate-950 px-4 py-1.5 text-[11px] font-semibold uppercase tracking-[0.24em] text-white transition hover:bg-brand hover:text-white"
            >
              Login
            </button>
          </div>
        </div>
      </header>

      {isMobileSidebarOpen ? (
        <div className="fixed inset-0 z-50 bg-slate-950/50 lg:hidden" onClick={() => setIsMobileSidebarOpen(false)}>
          <aside
            className="h-full w-[86vw] max-w-sm overflow-y-auto bg-[#eef3fa] px-4 py-6 shadow-2xl"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="mb-4 flex items-center justify-between">
              <p className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-600">Navigation</p>
              <button
                type="button"
                onClick={() => setIsMobileSidebarOpen(false)}
                className="rounded border border-slate-300 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-700"
              >
                Close
              </button>
            </div>
            <CreditCardSidebar
              section={section}
              activeOperation={activeOperation}
              queryParams={queryParams}
              onNavigate={() => setIsMobileSidebarOpen(false)}
            />
          </aside>
        </div>
      ) : null}

      <div className="flex w-full flex-col pb-8 lg:flex-row">
        <aside className="hidden w-full shrink-0 bg-[#eef3fa] px-4 py-6 lg:sticky lg:top-16 lg:block lg:h-[calc(100vh-4rem)] lg:w-56 lg:self-start lg:overflow-y-auto lg:border-r lg:border-slate-200 lg:px-4 lg:pr-4">
          <CreditCardSidebar section={section} activeOperation={activeOperation} queryParams={queryParams} />
        </aside>
        <main className="min-w-0 flex-1 px-3 pt-6 sm:px-6 lg:px-8 lg:pt-0">{children}</main>
      </div>
    </div>
  );
}
