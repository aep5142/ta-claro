import Link from "next/link";
import { CreditCardSidebar } from "@/components/credit-card-sidebar";
import { primarySections } from "@/lib/credit-card-config";
import { cn } from "@/lib/utils";

type AppShellProps = {
  children: React.ReactNode;
  section: "credit-cards" | "debit-cards" | "accounts" | "loans";
  activeOperation?: string;
};

export function AppShell({ children, section, activeOperation }: AppShellProps) {
  return (
    <div className="min-h-screen bg-surface">
      <header className="border-b border-border bg-surface/95 backdrop-blur">
        <div className="grid w-full grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-4 px-4 py-5 sm:px-6 lg:px-8">
          <Link href="/credit-cards/purchases?view=volume" className="justify-self-start">
            <p className="text-2xl font-semibold tracking-tight text-white">Ta-Claro</p>
          </Link>

          <nav className="flex flex-wrap items-center justify-center gap-7">
            {primarySections.map((item) => {
              const isActive = section === item.slug;

              return (
                <Link
                  key={item.slug}
                  href={item.href}
                  className={cn(
                    "border-b-2 pb-1 text-xs font-medium uppercase tracking-[0.28em] transition",
                    isActive
                      ? "border-brand text-white"
                      : "border-transparent text-muted hover:text-white"
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
              className="rounded-sm border border-white/20 bg-white px-4 py-2 text-xs font-semibold uppercase tracking-[0.24em] text-surface transition hover:bg-brand hover:text-white"
            >
              Login
            </button>
          </div>
        </div>
      </header>

      <div className="flex w-full flex-col gap-8 px-4 py-8 sm:px-6 lg:px-8 lg:flex-row">
        <aside className="w-full shrink-0 border-border lg:w-72 lg:border-r lg:pr-8">
          <CreditCardSidebar section={section} activeOperation={activeOperation} />
        </aside>
        <main className="min-w-0 flex-1">{children}</main>
      </div>
    </div>
  );
}
