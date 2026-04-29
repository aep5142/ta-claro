import Link from "next/link";
import { creditCardOperations } from "@/lib/credit-card-config";
import { cn } from "@/lib/utils";

type CreditCardSidebarProps = {
  section: "credit-cards" | "debit-cards" | "accounts" | "loans";
  activeOperation?: string;
};

export function CreditCardSidebar({ section, activeOperation }: CreditCardSidebarProps) {
  const sectionTitle =
    section === "credit-cards"
      ? "Credit Cards"
      : section === "debit-cards"
        ? "Debit Cards"
        : section === "accounts"
          ? "Accounts"
          : "Loans";

  return (
    <div className="rounded-3xl border border-border bg-panel p-4">
      <div className="mb-6">
        <h2 className="text-xl font-semibold text-white">{sectionTitle}</h2>
      </div>

      {section === "credit-cards" ? (
        <div className="space-y-2">
          {creditCardOperations.map((item) => {
            const isActive = activeOperation === item.slug;

            return (
              <Link
                key={item.slug}
                href={`/credit-cards/${item.slug}?view=${
                  item.slug === "operations-rate" ? "total-active-cards" : "volume"
                }`}
                className={cn(
                  "flex items-center justify-between rounded-2xl border px-4 py-3 text-sm transition",
                  isActive
                    ? "border-brand/60 bg-brand/10 text-white"
                    : "border-transparent bg-panelMuted text-muted hover:border-border hover:text-white"
                )}
              >
                <span>{item.label}</span>
              </Link>
            );
          })}
        </div>
      ) : (
        <div className="rounded-2xl border border-dashed border-border bg-panelMuted p-4 text-sm text-muted">
          Only the Credit Cards section is connected in v1. This route remains part of the shared shell.
        </div>
      )}
    </div>
  );
}
