"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import { fetchHomepageMetrics } from "@/lib/homepage-queries";
import { checkingAccountOperations } from "@/lib/checking-account-config";
import { creditCardOperations } from "@/lib/credit-card-config";
import {
  OptionalSignInButton,
  OptionalSignedIn,
  OptionalSignedOut,
  OptionalUserButton,
} from "@/lib/clerk-compat";
import { SiteFooter } from "@/components/site-footer";
import { debitCardOperations } from "@/lib/debit-card-config";
import { prepaidCardOperations, prepaidCustomerTypes } from "@/lib/prepaid-card-config";

const NAV = [
  { label: "Credit Cards", href: "/credit-cards" },
  { label: "Debit Cards", href: "/debit-cards" },
  { label: "Prepaid Cards", href: "/prepaid-cards" },
  { label: "Checking Accounts", href: "/checking-accounts" },
  { label: "Loans", href: "/loans" },
] as const;

const PRODUCTS = [
  {
    id: "credit",
    title: "Credit Cards",
    metrics: ["Purchases", "Cash Advances", "Fees", "Activation Metrics"],
    accent: "var(--home-mint)",
    href: "/credit-cards",
  },
  {
    id: "debit",
    title: "Debit Cards",
    metrics: ["Volume", "Transactions", "Avg. Ticket", "Activation"],
    accent: "var(--home-amber)",
    href: "/debit-cards",
  },
  {
    id: "prepaid",
    title: "Prepaid Cards",
    metrics: ["Natural Person", "Business", "ATM", "Activation"],
    accent: "var(--home-pink)",
    href: "/prepaid-cards",
  },
  {
    id: "checking",
    title: "Checking Accounts",
    metrics: ["Balances", "Accounts", "Average Balance", "UF"],
    accent: "var(--home-mint)",
    href: "/checking-accounts",
  },
  {
    id: "loans",
    title: "Loans",
    metrics: ["Consumer", "Mortgage", "Commercial", "Soon"],
    accent: "var(--home-amber)",
    href: "/loans",
  },
] as const;

const HERO_MOCK_BARS = [
  { name: "Bank 1", value: 98120, growth: "+7,2%" },
  { name: "Bank 2", value: 90450, growth: "+5,9%" },
  { name: "Bank 3", value: 86210, growth: "+4,8%" },
  { name: "Bank 4", value: 80170, growth: "+4,1%" },
  { name: "Bank 5", value: 74430, growth: "+3,7%" },
  { name: "Bank 6", value: 68910, growth: "+3,1%" },
  { name: "Bank 7", value: 64080, growth: "+2,8%" },
  { name: "Bank 8", value: 59200, growth: "+2,1%" },
  { name: "Bank 9", value: 55870, growth: "+1,7%" },
  { name: "Bank 10", value: 52110, growth: "+1,2%" },
] as const;


type LivePulseCase = {
  product: string;
  volume: string;
  growth: string;
};

type HeroBar = {
  name: string;
  color: string;
  value: number;
  growth: string;
};

type HomepagePrototypeProps = {
  navStyle?: "dark" | "white-shell";
  homeHref?: string;
};

function LoginButton({ compact = false }: { compact?: boolean }) {
  return (
    <>
      <OptionalSignedOut>
        <OptionalSignInButton>
          <button
            type="button"
            className={[
              "border border-white bg-white px-4 py-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--home-background)] transition-colors hover:border-[var(--home-mint)] hover:bg-[var(--home-mint)]",
              compact ? "px-3 py-1.5 text-[10px]" : "",
            ].join(" ")}
          >
            Login
          </button>
        </OptionalSignInButton>
      </OptionalSignedOut>
      <OptionalSignedIn>
        <OptionalUserButton afterSignOutUrl="/" />
      </OptionalSignedIn>
    </>
  );
}

function navDropdownItems(href: string) {
  if (href === "/credit-cards") {
    return creditCardOperations.map((item) => ({
      label: item.label,
      href: `/credit-cards/${item.slug}?view=${item.slug === "total-activation-rate" ? "total-active-cards" : "volume"}`,
    }));
  }

  if (href === "/debit-cards") {
    return debitCardOperations.map((item) => ({
      label: item.label,
      href: `/debit-cards/${item.slug}?view=${item.slug === "total-activation-rate" ? "total-active-cards" : "volume"}`,
    }));
  }

  if (href === "/checking-accounts") {
    return checkingAccountOperations.map((item) => ({
      label: item.label,
      href: `/checking-accounts/${item.slug}?view=volume`,
    }));
  }

  if (href === "/prepaid-cards") {
    return prepaidCustomerTypes.flatMap((customerType) =>
      prepaidCardOperations.map((item) => ({
        label: `${customerType.label}: ${item.label}`,
        href: `/prepaid-cards/${customerType.slug}/${item.slug}?view=${
          item.slug === "total-activation-rate" ? "total-active-cards" : "volume"
        }`,
      }))
    );
  }

  return [];
}

function HomeNav({ navStyle = "dark", homeHref = "/" }: HomepagePrototypeProps) {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);

  if (navStyle === "white-shell") {
    return (
      <header className="sticky top-0 z-50 bg-white text-slate-950 shadow-sm">
        <div className="flex h-16 w-full items-center justify-between gap-3 px-4 sm:px-6 lg:hidden">
          <Link href={homeHref} className="min-w-0">
            <p className="truncate text-lg font-semibold tracking-tight text-slate-950">Taclaro</p>
          </Link>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => setIsMobileMenuOpen(true)}
              className="rounded border border-slate-300 px-3 py-1.5 text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-700"
            >
              Menu
            </button>
            <OptionalSignedOut>
              <OptionalSignInButton>
                <button
                  type="button"
                  className="rounded-sm border border-slate-950 bg-slate-950 px-3 py-1.5 text-[10px] font-semibold uppercase tracking-[0.18em] text-white transition hover:bg-[var(--home-mint)] hover:text-slate-950"
                >
                  Login
                </button>
              </OptionalSignInButton>
            </OptionalSignedOut>
            <OptionalSignedIn>
              <OptionalUserButton afterSignOutUrl="/" />
            </OptionalSignedIn>
          </div>
        </div>

        <div className="border-t border-slate-200 px-4 py-2 sm:px-6 lg:hidden">
          <nav className="flex items-center gap-5 overflow-x-auto whitespace-nowrap pb-0.5">
            {NAV.map((item) => (
              <Link
                key={item.label}
                href={item.href}
                className="border-b-2 border-transparent pb-0.5 text-[10px] font-medium uppercase tracking-[0.18em] text-slate-700 transition hover:text-slate-950"
              >
                {item.label}
              </Link>
            ))}
          </nav>
        </div>

        <div className="hidden h-16 w-full grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-4 px-4 sm:px-6 lg:grid lg:px-8">
          <Link href={homeHref} className="justify-self-start">
            <p className="text-xl font-semibold tracking-tight text-slate-950">Taclaro</p>
          </Link>

          <nav className="flex flex-wrap items-center justify-center gap-7">
            {NAV.map((item) => {
              const dropdownItems = navDropdownItems(item.href);

              return (
                <div key={item.label} className="group relative">
                  <Link
                    href={item.href}
                    className="border-b-2 border-transparent pb-0.5 text-[11px] font-semibold uppercase tracking-[0.28em] text-slate-700 transition hover:text-slate-950 focus-visible:text-slate-950"
                  >
                    {item.label}
                  </Link>
                  {dropdownItems.length > 0 ? (
                    <div className="pointer-events-none absolute left-0 top-full z-40 hidden w-72 group-hover:block group-focus-within:block">
                      <div className="pointer-events-auto rounded-xl border border-slate-200 bg-white p-2 shadow-xl">
                        {dropdownItems.map((dropdownItem) => (
                          <Link
                            key={dropdownItem.href}
                            href={dropdownItem.href}
                            className="block rounded-md px-3 py-2 text-xs font-medium text-slate-700 transition hover:bg-slate-100 hover:text-slate-950"
                          >
                            {dropdownItem.label}
                          </Link>
                        ))}
                      </div>
                    </div>
                  ) : null}
                </div>
              );
            })}
          </nav>

          <div className="justify-self-end">
            <OptionalSignedOut>
              <OptionalSignInButton>
                <button
                  type="button"
                  className="rounded-sm border border-slate-950 bg-slate-950 px-4 py-1.5 text-[11px] font-semibold uppercase tracking-[0.24em] text-white transition hover:bg-[var(--home-mint)] hover:text-slate-950"
                >
                  Login
                </button>
              </OptionalSignInButton>
            </OptionalSignedOut>
            <OptionalSignedIn>
              <OptionalUserButton afterSignOutUrl="/" />
            </OptionalSignedIn>
          </div>
        </div>

        {isMobileMenuOpen ? (
          <div className="fixed inset-0 z-50 bg-slate-950/50 lg:hidden" onClick={() => setIsMobileMenuOpen(false)}>
            <aside
              className="h-full w-[86vw] max-w-sm overflow-y-auto bg-[#eef3fa] px-4 py-6 shadow-2xl"
              onClick={(event) => event.stopPropagation()}
            >
              <div className="mb-4 flex items-center justify-between">
                <p className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-600">Navigation</p>
                <button
                  type="button"
                  onClick={() => setIsMobileMenuOpen(false)}
                  className="rounded border border-slate-300 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-700"
                >
                  Close
                </button>
              </div>
              <div className="space-y-4">
                {NAV.map((item) => (
                  <Link
                    key={item.label}
                    href={item.href}
                    onClick={() => setIsMobileMenuOpen(false)}
                    className="block border-l-2 border-transparent pl-4 text-[15px] text-slate-700 transition hover:border-[var(--home-mint)] hover:text-slate-950"
                  >
                    {item.label}
                  </Link>
                ))}
              </div>
            </aside>
          </div>
        ) : null}
      </header>
    );
  }

  return (
    <header className="sticky top-0 z-50 border-b border-[var(--home-rule)] bg-[color:rgb(18_28_45_/_0.88)] backdrop-blur">
      <div className="mx-auto flex h-16 max-w-[1400px] items-center justify-between gap-4 px-4 sm:px-6">
        <Link href={homeHref} className="flex items-baseline gap-2">
          <span className="font-[family:var(--font-home-display)] text-2xl tracking-tight text-[var(--home-foreground)]">
            Taclaro
          </span>
          <span className="h-1.5 w-1.5 bg-[var(--home-mint)] home-pulse-dot" />
        </Link>
        <nav className="hidden items-center gap-7 md:flex">
          {NAV.map((item) => (
            <Link
              key={item.label}
              href={item.href}
              className="text-[11px] font-semibold uppercase tracking-[0.2em] text-[var(--home-foreground)]/90 transition-colors hover:text-[var(--home-mint)]"
            >
              {item.label}
            </Link>
          ))}
        </nav>
        <div className="hidden md:block">
          <LoginButton />
        </div>
        <div className="md:hidden">
          <LoginButton compact />
        </div>
      </div>
    </header>
  );
}

function MiniChart({ bars }: { bars: HeroBar[] }) {
  const safeBars = bars.length
    ? bars
    : [{ name: "Loading", color: "var(--home-mint)", value: 1, growth: "N/A" }];
  const width = 720;
  const height = 520;
  const padding = { left: 20, right: 220, top: 18, bottom: 18 };
  const maxValue = Math.max(...safeBars.map((bar) => bar.value));
  const safeMaxValue = Math.max(maxValue, 1);
  const trackWidth = width - padding.left - padding.right;
  const rowHeight = (height - padding.top - padding.bottom) / safeBars.length;
  const barHeight = Math.min(24, rowHeight * 0.58);

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="h-full w-full">
      {safeBars.map((bar, index) => {
        const y = padding.top + index * rowHeight + (rowHeight - barHeight) / 2;
        const barWidth = (bar.value / safeMaxValue) * trackWidth;
        return (
          <g key={bar.name}>
            <rect x={padding.left} y={y} width={trackWidth} height={barHeight} fill="var(--home-surface)" rx={3} />
            <rect
              x={padding.left}
              y={y}
              width={barWidth}
              height={barHeight}
              fill={bar.color}
              rx={3}
              className="home-draw-line"
              style={{ animationDelay: `${index * 0.09}s` }}
            />
            <text
              x={padding.left + barWidth / 2}
              y={y + barHeight / 2 + 4}
              fontSize="14"
              fill="var(--home-background)"
              fontFamily="var(--font-home-mono)"
              textAnchor="middle"
            >
              {`$${bar.value.toLocaleString("de-DE")}`}
            </text>
            <text
              x={padding.left + barWidth + 8}
              y={y + barHeight / 2 + 5}
              fontSize="12"
              fill="var(--home-foreground)"
              fontFamily="var(--font-home-mono)"
            >
              {bar.name} ({bar.growth})
            </text>
          </g>
        );
      })}
    </svg>
  );
}

function HeroSection({
  bars,
  monthLabel,
}: {
  bars: HeroBar[];
  monthLabel: string;
}) {
  return (
    <section className="relative flex min-h-[calc(100vh-4rem)] items-center border-b border-[var(--home-rule)]">
      <div className="mx-auto grid max-w-[1400px] grid-cols-12 gap-0 px-4 py-12 sm:px-6 md:py-14">
        <div className="col-span-12 lg:col-span-7 lg:pr-10">
          <h1 className="font-[family:var(--font-home-display)] text-[clamp(2.6rem,6.5vw,5.4rem)] leading-[0.95] tracking-tight text-[var(--home-foreground)]">
            <span className="text-[var(--home-mint)]">Instant benchmark for Chilean banking,</span>
            <span className="mt-3 block text-[clamp(1.9rem,4.8vw,3.4rem)] leading-[1.02] text-[var(--home-foreground)]">
              without downloading a single file
            </span>
          </h1>
          <p className="mt-8 max-w-xl text-base leading-relaxed text-[var(--home-muted)]">
            What used to mean downloading spreadsheets, normalizing UF, cleaning issuer names, and
            building charts now starts from one screen.
          </p>

          <dl className="mt-12 grid grid-cols-1 gap-px bg-[var(--home-rule)] sm:grid-cols-3">
            {[
              ["20+", "Institutions"],
              ["17", "Years of data"],
              ["UF", "Deflated"],
            ].map(([key, value]) => (
              <div key={value} className="bg-[var(--home-background)] p-5">
                <dt className="font-[family:var(--font-home-display)] text-3xl text-[var(--home-foreground)]">
                  {key}
                </dt>
                <dd className="mt-1 font-[family:var(--font-home-mono)] text-[11px] uppercase tracking-[0.16em] text-[var(--home-muted)]">
                  {value}
                </dd>
              </div>
            ))}
          </dl>
        </div>

        <div className="col-span-12 mt-10 lg:col-span-5 lg:mt-0 lg:flex">
          <div className="relative flex w-full flex-col border border-[var(--home-rule)] bg-[var(--home-surface)]">
            <div className="flex items-center justify-between border-b border-[var(--home-rule)] px-5 py-3">
              <div>
                <div className="font-[family:var(--font-home-mono)] text-[10px] uppercase tracking-[0.18em] text-[var(--home-muted)]">
                  {monthLabel}
                </div>
                <div className="mt-1 font-[family:var(--font-home-display)] text-lg text-[var(--home-foreground)]">
                  Average Purchase with Credit Card and Growth YoY
                </div>
              </div>
            </div>
            <div className="w-full flex-1">
              <MiniChart bars={bars} />
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function ProductsSection() {
  return (
    <section className="border-b border-[var(--home-rule)] py-14 sm:py-16">
      <div className="mx-auto max-w-[1400px] px-4 sm:px-6">
        <div className="mb-10 flex flex-col items-start justify-between gap-6 md:flex-row md:items-end">
          <div>
            <div className="font-[family:var(--font-home-mono)] text-[11px] uppercase tracking-[0.2em] text-[var(--home-mint)]">
              02 — Coverage
            </div>
            <h2 className="mt-4 font-[family:var(--font-home-display)] text-4xl leading-tight text-[var(--home-foreground)] md:text-5xl">
              Five products.
              <span className="italic text-[var(--home-muted)]"> One view.</span>
            </h2>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-5">
          {PRODUCTS.map((product) => (
            <Link
              key={product.id}
              href={product.href}
              className="group relative flex h-full flex-col justify-between rounded-2xl border border-[var(--home-rule)] bg-[color:rgb(12_24_41_/_0.78)] p-6 transition-all hover:-translate-y-1 hover:border-[var(--home-mint)] hover:bg-[var(--home-surface)]"
            >
              <div>
                <div className="flex items-start justify-between gap-4">
                  <h3 className="font-[family:var(--font-home-display)] text-2xl leading-tight text-[var(--home-foreground)]">
                    {product.title}
                  </h3>
                  <span
                    className="mt-1 h-3 w-3 shrink-0 transition-transform group-hover:scale-125"
                    style={{ background: product.accent }}
                  />
                </div>
              </div>
              <ul className="mt-10 space-y-1.5 font-[family:var(--font-home-mono)] text-[11px] uppercase tracking-wider text-[var(--home-muted)]">
                {product.metrics.map((metric) => (
                  <li key={metric} className="flex items-center gap-2">
                    <span className="h-px w-3 bg-[var(--home-rule)]" />
                    {metric}
                  </li>
                ))}
              </ul>
              <div className="mt-8 flex items-center justify-between">
                <div
                  className="h-1 w-16 origin-left transition-all group-hover:w-24"
                  style={{ background: product.accent }}
                />
                <span className="text-[10px] font-semibold uppercase tracking-[0.18em] text-[var(--home-muted)]">
                  Explore →
                </span>
              </div>
            </Link>
          ))}
        </div>
      </div>
    </section>
  );
}

function WorkflowSection() {
  const before = [
    "Download CMF spreadsheets",
    "Clean and join multiple tabs",
    "Convert CLP to UF month by month",
    "Build charts manually",
    "Repeat every month",
  ];
  const after = ["Open Taclaro", "Choose a metric", "Get the insights"];

  return (
    <section id="methodology" className="border-b border-[var(--home-rule)] bg-[var(--home-surface)] py-14 sm:py-16">
      <div className="mx-auto max-w-[1400px] px-4 sm:px-6">
        <div className="grid grid-cols-12 gap-8">
          <div className="col-span-12 lg:col-span-4">
            <div className="font-[family:var(--font-home-mono)] text-[11px] uppercase tracking-[0.2em] text-[var(--home-mint)]">
              03 — Method
            </div>
            <h2 className="mt-4 font-[family:var(--font-home-display)] text-4xl leading-tight text-[var(--home-foreground)] md:text-5xl">
              From hours
              <span className="block italic text-[var(--home-mint)]">to seconds.</span>
            </h2>
            <p className="mt-6 max-w-sm text-sm leading-relaxed text-[var(--home-muted)]">
              The same UF-adjusted methodology, but without manual cleanup loops before real analysis.
            </p>
          </div>
          <div className="col-span-12 grid grid-cols-1 gap-px bg-[var(--home-rule)] lg:col-span-8 lg:grid-cols-2">
            <div className="bg-[var(--home-background)] p-8">
              <div className="flex items-center gap-3 font-[family:var(--font-home-mono)] text-[11px] uppercase tracking-[0.18em] text-[var(--home-pink)]">
                <span className="h-1.5 w-6 bg-[var(--home-pink)]" />
                Before
              </div>
              <ol className="mt-6 space-y-4">
                {before.map((step, index) => (
                  <li key={step} className="flex items-center gap-3 border-b border-[var(--home-rule)] pb-3 last:border-b-0">
                    <span className="font-[family:var(--font-home-mono)] text-[11px] text-[var(--home-muted)]">
                      {String(index + 1).padStart(2, "0")}
                    </span>
                    <span className="text-sm text-[var(--home-muted)] line-through decoration-[var(--home-pink)]/60">
                      {step}
                    </span>
                  </li>
                ))}
              </ol>
            </div>
            <div className="bg-[var(--home-background)] p-8">
              <div className="flex items-center gap-3 font-[family:var(--font-home-mono)] text-[13px] font-semibold uppercase tracking-[0.16em] text-[var(--home-mint)]">
                <span className="h-1.5 w-6 bg-[var(--home-mint)]" />
                With Taclaro
              </div>
              <ol className="mt-6 space-y-4">
                {after.map((step, index) => (
                  <li key={step} className="flex items-center gap-3 border-b border-[var(--home-rule)] pb-3 last:border-b-0">
                    <span className="font-[family:var(--font-home-mono)] text-[11px] text-[var(--home-mint)]">
                      {String(index + 1).padStart(2, "0")}
                    </span>
                    <span className="text-base text-[var(--home-foreground)]">{step}</span>
                  </li>
                ))}
              </ol>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function LivePulseSection({ cases, monthLabel }: { cases: LivePulseCase[]; monthLabel: string }) {
  const [activeIndex, setActiveIndex] = useState(0);
  const [isVisible, setIsVisible] = useState(true);
  const safeCases = cases.length
    ? cases
    : [{ product: "Loading", volume: "Loading", growth: "N/A" }];

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      setIsVisible(false);
      window.setTimeout(() => {
        setActiveIndex((current) => (current + 1) % safeCases.length);
        setIsVisible(true);
      }, 220);
    }, 2800);

    return () => window.clearInterval(intervalId);
  }, [safeCases.length]);

  const activeCase = safeCases[activeIndex % safeCases.length];

  return (
    <section className="border-b border-[var(--home-rule)] bg-[var(--home-surface)] py-16 sm:py-20">
      <div className="mx-auto max-w-[1400px] px-4 sm:px-6">
        <div className="mb-8 flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <div className="font-[family:var(--font-home-mono)] text-[11px] uppercase tracking-[0.2em] text-[var(--home-mint)]">
              04 — Live pulse
            </div>
            <h2 className="mt-3 font-[family:var(--font-home-display)] text-4xl text-[var(--home-foreground)] sm:text-5xl">
              Product momentum at a glance
            </h2>
          </div>
          <div className="font-[family:var(--font-home-mono)] text-[11px] uppercase tracking-[0.18em] text-[var(--home-muted)]">
            {monthLabel}
          </div>
        </div>
        <div className="grid grid-cols-1 gap-px bg-[var(--home-rule)] md:grid-cols-3">
          {[
            { label: "Product", value: activeCase.product, tone: "text-[var(--home-foreground)]" },
            { label: "Volume", value: activeCase.volume, tone: "text-[var(--home-foreground)]" },
            { label: "Growth YoY", value: activeCase.growth, tone: "text-[var(--home-mint)]" },
          ].map((item, index) => (
            <div
              key={item.label}
              className={`relative flex h-[188px] flex-col items-center justify-center overflow-hidden bg-[var(--home-background)] px-6 py-7 transition-all duration-500 ${
                isVisible ? "translate-y-0 opacity-100" : "translate-y-2 opacity-0"
              }`}
              style={{ transitionDelay: `${index * 60}ms` }}
            >
              <div className="pointer-events-none absolute inset-x-0 top-0 h-px bg-[linear-gradient(90deg,transparent,var(--home-mint),transparent)] opacity-70" />
              <div className="absolute left-0 right-0 top-8 text-center text-[10px] font-semibold uppercase tracking-[0.2em] text-[var(--home-muted)]">
                {item.label}
              </div>
              <div className={`text-center text-2xl font-semibold leading-[1.12] sm:text-3xl ${item.tone}`}>
                {item.value}
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function CallToActionSection() {
  return (
    <section className="relative border-b border-[var(--home-rule)] py-16 sm:py-20">
      <div className="mx-auto max-w-[1400px] px-4 sm:px-6">
        <div className="grid grid-cols-12 items-center gap-8">
          <div className="col-span-12 lg:col-span-8">
            <h2 className="font-[family:var(--font-home-display)] text-[clamp(2.5rem,6vw,5rem)] leading-[0.95] tracking-tight text-[var(--home-foreground)]">
              Stop fighting spreadsheets.
              <span className="block italic text-[var(--home-mint)]">Start comparing.</span>
            </h2>
          </div>
          <div className="col-span-12 lg:col-span-4">
            <Link
              href="/credit-cards/purchases?view=volume"
              className="group flex items-center justify-between bg-[var(--home-mint)] px-6 py-5 text-[12px] font-semibold uppercase tracking-[0.18em] text-[var(--home-background)] transition-colors hover:bg-white"
            >
              Enter Taclaro
              <span className="transition-transform group-hover:translate-x-1">→</span>
            </Link>
          </div>
        </div>

        <div
          className="mt-12 grid gap-1 opacity-70"
          style={{ gridTemplateColumns: "repeat(24, minmax(0, 1fr))" }}
        >
          {Array.from({ length: 48 }).map((_, index) => (
            <div
              key={index}
              className="h-2"
              style={{
                background:
                  index % 7 === 0
                    ? "var(--home-mint)"
                    : index % 11 === 0
                      ? "var(--home-pink)"
                      : "var(--home-rule)",
              }}
            />
          ))}
        </div>
      </div>
    </section>
  );
}

export function HomepagePrototype({
  navStyle = "dark",
  homeHref = "/",
}: HomepagePrototypeProps) {
  const [livePulseCases, setLivePulseCases] = useState<LivePulseCase[]>([]);
  const [livePulseMonthLabel, setLivePulseMonthLabel] = useState("Latest month");
  const [shouldLoadLivePulse, setShouldLoadLivePulse] = useState(false);
  const hasLoadedLivePulseRef = useRef(false);
  const livePulseSectionRef = useRef<HTMLDivElement | null>(null);

  const heroBars = useMemo(
    () =>
      HERO_MOCK_BARS.map((bar, index) => {
        const barPalette = ["var(--home-mint)", "var(--home-amber)", "var(--home-pink)"] as const;
        return { ...bar, color: barPalette[index % barPalette.length] };
      }),
    []
  );

  const heroMonthLabel = "Latest month (Mock)";

  useEffect(() => {
    const sectionNode = livePulseSectionRef.current;
    if (!sectionNode) {
      return;
    }

    if (typeof window === "undefined" || typeof window.IntersectionObserver === "undefined") {
      setShouldLoadLivePulse(true);
      return;
    }

    const observer = new window.IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          setShouldLoadLivePulse(true);
          observer.disconnect();
        }
      },
      { rootMargin: "400px 0px" }
    );
    observer.observe(sectionNode);

    return () => {
      observer.disconnect();
    };
  }, []);

  useEffect(() => {
    if (!shouldLoadLivePulse || hasLoadedLivePulseRef.current) {
      return;
    }

    let cancelled = false;
    hasLoadedLivePulseRef.current = true;

    async function loadLivePulse() {
      try {
        const payload = await fetchHomepageMetrics();
        if (!cancelled) {
          setLivePulseCases(payload.livePulseCases);
          setLivePulseMonthLabel(payload.livePulseMonthLabel);
        }
      } catch {
        if (!cancelled) {
          setLivePulseCases([]);
        }
      }
    }

    void loadLivePulse();

    return () => {
      cancelled = true;
    };
  }, [shouldLoadLivePulse]);

  return (
    <div className="min-h-screen bg-[var(--home-background)] text-[var(--home-foreground)]">
      <HomeNav navStyle={navStyle} homeHref={homeHref} />
      <main>
        <HeroSection bars={heroBars} monthLabel={heroMonthLabel} />
        <ProductsSection />
        <WorkflowSection />
        <div ref={livePulseSectionRef}>
          <LivePulseSection cases={livePulseCases} monthLabel={livePulseMonthLabel} />
        </div>
        <CallToActionSection />
      </main>
      <SiteFooter />
    </div>
  );
}
