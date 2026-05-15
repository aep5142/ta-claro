"use client";

import Link from "next/link";
import { cn } from "@/lib/utils";

const FOOTER_PRODUCTS = [
  { label: "Credit Cards", href: "/credit-cards" },
  { label: "Debit Cards", href: "/debit-cards" },
  { label: "Prepaid Cards", href: "/prepaid-cards" },
  { label: "Checking Accounts", href: "/checking-accounts" },
  { label: "Loans", href: "/loans" },
] as const;

type SiteFooterProps = {
  className?: string;
};

export function SiteFooter({ className }: SiteFooterProps) {
  return (
    <footer className={cn("border-t border-[var(--home-rule)] bg-[var(--home-background)] pb-6 pt-10", className)}>
      <div className="mx-auto grid max-w-[1400px] grid-cols-12 gap-8 px-4 sm:px-6">
        <div className="col-span-12 md:col-span-5">
          <div className="flex items-baseline gap-2">
            <span className="font-[family:var(--font-home-display)] text-3xl text-[var(--home-foreground)]">
              Taclaro
            </span>
            <span className="h-1.5 w-1.5 bg-[var(--home-mint)]" />
          </div>
          <p className="mt-4 max-w-sm text-sm text-[var(--home-muted)]">Instant benchmark for Chilean banking.</p>
        </div>

        <div className="col-span-6 md:col-span-2">
          <div className="font-[family:var(--font-home-mono)] text-[10px] uppercase tracking-[0.18em] text-[var(--home-muted)]">
            Products
          </div>
          <ul className="mt-4 space-y-2 text-sm text-[var(--home-foreground)]">
            {FOOTER_PRODUCTS.map((item) => (
              <li key={item.label}>
                <Link href={item.href} className="hover:text-[var(--home-mint)]">
                  {item.label}
                </Link>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </footer>
  );
}
