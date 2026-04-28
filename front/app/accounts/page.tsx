import { AppShell } from "@/components/app-shell";
import { PlaceholderPanel } from "@/components/placeholder-panel";

export default function AccountsPage() {
  return (
    <AppShell section="accounts">
      <PlaceholderPanel
        title="Accounts"
        description="This section is a placeholder in v1 while the credit-card demo is the active scope."
      />
    </AppShell>
  );
}
