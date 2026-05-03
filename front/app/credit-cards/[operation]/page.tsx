import { notFound } from "next/navigation";
import { AppShell } from "@/components/app-shell";
import { CreditCardsDashboard } from "@/components/credit-cards-dashboard";
import { operationFromSlug } from "@/lib/credit-card-config";

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

export default async function CreditCardsPage({ params, searchParams }: PageProps) {
  const { operation } = await params;
  const resolvedOperation = operationFromSlug(operation);

  if (!resolvedOperation) {
    notFound();
  }

  const { view, start, end, uf } = await searchParams;

  return (
    <AppShell section="credit-cards" activeOperation={operation} queryParams={{ view, start, end, uf }}>
      <CreditCardsDashboard
        operation={resolvedOperation}
        initialView={view}
        startMonthParam={start}
        endMonthParam={end}
        ufParam={uf}
      />
    </AppShell>
  );
}
