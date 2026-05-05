import { notFound } from "next/navigation";
import { AppShell } from "@/components/app-shell";
import { DebitCardsDashboard } from "@/components/debit-cards-dashboard";
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

export default async function DebitCardsOperationPage({ params, searchParams }: PageProps) {
  const { operation } = await params;
  const resolvedOperation = operationFromSlug(operation);

  if (!resolvedOperation) {
    notFound();
  }

  const { view, start, end, uf } = await searchParams;

  return (
    <AppShell section="debit-cards" activeOperation={operation} queryParams={{ view, start, end, uf }}>
      <DebitCardsDashboard
        operation={resolvedOperation}
        initialView={view}
        startMonthParam={start}
        endMonthParam={end}
        ufParam={uf}
      />
    </AppShell>
  );
}
