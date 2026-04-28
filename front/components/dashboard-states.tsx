import { cn } from "@/lib/utils";

type StateProps = {
  title?: string;
  description?: string;
  label?: string;
  compact?: boolean;
};

export function LoadingState({ label = "Loading", compact = false }: StateProps) {
  return (
    <div
      className={cn(
        "rounded-3xl border border-border bg-panel text-center text-muted",
        compact ? "px-6 py-10" : "px-8 py-16"
      )}
    >
      {label}
    </div>
  );
}

export function ErrorState({ title = "Error", description, compact = false }: StateProps) {
  return (
    <div
      className={cn(
        "rounded-3xl border border-rose-500/30 bg-rose-500/10 text-rose-100",
        compact ? "px-6 py-5" : "px-8 py-16"
      )}
    >
      <h2 className="text-lg font-semibold">{title}</h2>
      {description ? <p className="mt-2 text-sm text-rose-100/80">{description}</p> : null}
    </div>
  );
}

export function EmptyState({ title = "No data", description, compact = false }: StateProps) {
  return (
    <div
      className={cn(
        "rounded-3xl border border-border bg-panelMuted text-center",
        compact ? "px-6 py-10" : "px-8 py-16"
      )}
    >
      <h2 className="text-lg font-semibold text-white">{title}</h2>
      {description ? <p className="mt-2 text-sm text-muted">{description}</p> : null}
    </div>
  );
}
