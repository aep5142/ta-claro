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
      <header className="border-b border-border bg-[#07101c]/95 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center gap-6 px-4 py-4 sm:px-6">
          <Link href="/credit-cards/purchases?view=volume" className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-brand/15 text-lg font-semibold text-brand">
              TC
            </div>
            <div>
              <p className="text-lg font-semibold tracking-tight text-white">Ta-Claro</p>
              <p className="text-xs text-muted">Financial market analytics</p>
            </div>
          </Link>

          <nav className="ml-auto flex flex-wrap gap-2">
            {primarySections.map((item) => {
              const isActive = section === item.slug;

              return (
                <Link
                  key={item.slug}
                  href={item.href}
                  className={cn(
                    "rounded-full border px-4 py-2 text-sm font-medium transition",
                    isActive
                      ? "border-brand/60 bg-brand/10 text-white"
                      : "border-border bg-panel text-muted hover:border-brand/40 hover:text-white"
                  )}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>
        </div>
      </header>

      <div className="mx-auto flex max-w-7xl flex-col gap-6 px-4 py-6 sm:px-6 lg:flex-row">
        <aside className="w-full shrink-0 lg:w-72">
          <CreditCardSidebar section={section} activeOperation={activeOperation} />
        </aside>
        <main className="min-w-0 flex-1">{children}</main>
      </div>
    </div>
  );
}
