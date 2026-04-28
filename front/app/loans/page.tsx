import { AppShell } from "@/components/app-shell";
import { PlaceholderPanel } from "@/components/placeholder-panel";

export default function LoansPage() {
  return (
    <AppShell section="loans">
      <PlaceholderPanel
        title="Loans"
        description="This section is reserved for a later phase and has no data integration in v1."
      />
    </AppShell>
  );
}
