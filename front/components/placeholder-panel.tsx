type PlaceholderPanelProps = {
  title: string;
  description: string;
};

export function PlaceholderPanel({ title, description }: PlaceholderPanelProps) {
  return (
    <section className="rounded-3xl border border-border bg-panel p-8">
      <p className="text-xs font-semibold uppercase tracking-[0.24em] text-brand">Placeholder</p>
      <h1 className="mt-3 text-3xl font-semibold text-white">{title}</h1>
      <p className="mt-4 max-w-2xl text-sm leading-6 text-muted">{description}</p>
    </section>
  );
}
