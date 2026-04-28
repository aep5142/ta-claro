import { AppShell } from "@/components/app-shell";
import { PlaceholderPanel } from "@/components/placeholder-panel";

export default function DebitCardsPage() {
  return (
    <AppShell section="debit-cards">
      <PlaceholderPanel
        title="Debit Cards"
        description="This section is visible in v1 but not yet wired to a live dataset."
      />
    </AppShell>
  );
}
