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
    <div className="py-2">
      <div className="mb-8">
        <h2 className="text-xs font-semibold uppercase tracking-[0.28em] text-slate-500">{sectionTitle}</h2>
      </div>

      {section === "credit-cards" ? (
        <div className="space-y-4">
          {creditCardOperations.map((item) => {
            const isActive = activeOperation === item.slug;

            return (
              <Link
                key={item.slug}
                href={`/credit-cards/${item.slug}?view=${
                  item.slug === "total-activation-rate" ? "total-active-cards" : "volume"
                }`}
                className={cn(
                  "block border-l-2 pl-4 text-[15px] transition",
                  isActive ? "border-brand font-semibold text-slate-950" : "border-transparent text-slate-700 hover:text-slate-950"
                )}
              >
                <span>{item.label}</span>
              </Link>
            );
          })}
        </div>
      ) : (
        <div className="max-w-[18rem] text-sm leading-6 text-slate-700">
          Only the Credit Cards section is connected in v1. This route remains part of the shared shell.
        </div>
      )}
    </div>
  );
}
