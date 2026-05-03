import { AppShell } from "@/components/app-shell";
import { PlaceholderPanel } from "@/components/placeholder-panel";

type AccountsPageProps = {
  searchParams: Promise<{
    view?: string;
    start?: string;
    end?: string;
    uf?: string;
  }>;
};

export default async function AccountsPage({ searchParams }: AccountsPageProps) {
  const { view, start, end, uf } = await searchParams;
  return (
    <AppShell section="accounts" queryParams={{ view, start, end, uf }}>
      <PlaceholderPanel
        title="Accounts"
        description="This section is a placeholder in v1 while the credit-card demo is the active scope."
      />
    </AppShell>
  );
}
