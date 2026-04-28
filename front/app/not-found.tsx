import { AppShell } from "@/components/app-shell";
import { PlaceholderPanel } from "@/components/placeholder-panel";

export default function NotFoundPage() {
  return (
    <AppShell section="credit-cards">
      <PlaceholderPanel
        title="Page not found"
        description="The requested route does not exist in the current Ta-Claro demo."
      />
    </AppShell>
  );
}
