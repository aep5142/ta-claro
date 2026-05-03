import { AppShell } from "@/components/app-shell";
import { PlaceholderPanel } from "@/components/placeholder-panel";

type DebitCardsPageProps = {
  searchParams: Promise<{
    view?: string;
    start?: string;
    end?: string;
    uf?: string;
  }>;
};

export default async function DebitCardsPage({ searchParams }: DebitCardsPageProps) {
  const { view, start, end, uf } = await searchParams;
  return (
    <AppShell section="debit-cards" queryParams={{ view, start, end, uf }}>
      <PlaceholderPanel
        title="Debit Cards"
        description="This section is visible in v1 but not yet wired to a live dataset."
      />
    </AppShell>
  );
}
