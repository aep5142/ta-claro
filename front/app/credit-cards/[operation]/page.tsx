import { notFound } from "next/navigation";
import { AppShell } from "@/components/app-shell";
import { CreditCardsDashboard } from "@/components/credit-cards-dashboard";
import { operationFromSlug, operationSlugs } from "@/lib/credit-card-config";

type PageProps = {
  params: Promise<{
    operation: string;
  }>;
  searchParams: Promise<{
    view?: string;
  }>;
};

export default async function CreditCardsPage({ params, searchParams }: PageProps) {
  const { operation } = await params;
  const resolvedOperation = operationFromSlug(operation);

  if (!resolvedOperation) {
    notFound();
  }

  const { view } = await searchParams;

  return (
    <AppShell section="credit-cards" activeOperation={operation}>
      <CreditCardsDashboard
        operation={resolvedOperation}
        operationSlug={operation as (typeof operationSlugs)[number]}
        initialView={view}
      />
    </AppShell>
  );
}
