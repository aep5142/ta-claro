import { AppShell } from "@/components/app-shell";
import { PlaceholderPanel } from "@/components/placeholder-panel";

type LoansPageProps = {
  searchParams: Promise<{
    view?: string;
    start?: string;
    end?: string;
    uf?: string;
  }>;
};

export default async function LoansPage({ searchParams }: LoansPageProps) {
  const { view, start, end, uf } = await searchParams;
  return (
    <AppShell section="loans" queryParams={{ view, start, end, uf }}>
      <PlaceholderPanel
        title="Loans"
        description="This section is reserved for a later phase and has no data integration in v1."
      />
    </AppShell>
  );
}
