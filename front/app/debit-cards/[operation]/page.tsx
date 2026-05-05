import { notFound } from "next/navigation";
import { AppShell } from "@/components/app-shell";
import { PlaceholderPanel } from "@/components/placeholder-panel";
import { operationFromSlug } from "@/lib/debit-card-config";

type PageProps = {
  params: Promise<{
    operation: string;
  }>;
  searchParams: Promise<{
    view?: string;
    start?: string;
    end?: string;
    uf?: string;
  }>;
};

const pageTitleByOperation = {
  "Debit Transactions": "Debit Transactions",
  "ATM Withdrawals": "ATM Withdrawals",
  "Total Activation Rate": "Operation Metrics",
} as const;

export default async function DebitCardsOperationPage({ params, searchParams }: PageProps) {
  const { operation } = await params;
  const resolvedOperation = operationFromSlug(operation);

  if (!resolvedOperation) {
    notFound();
  }

  const { view, start, end, uf } = await searchParams;

  return (
    <AppShell section="debit-cards" activeOperation={operation} queryParams={{ view, start, end, uf }}>
      <PlaceholderPanel
        title={pageTitleByOperation[resolvedOperation]}
        description="Debit Cards includes debit cards and ATM-only cards. Full dashboard wiring is in progress."
      />
    </AppShell>
  );
}
