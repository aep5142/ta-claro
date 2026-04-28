import { formatMoney } from "@/lib/formatters";

type BankSelectorProps = {
  banks: Array<{
    institutionCode: string;
    institutionName: string;
    latestValue: number | null;
  }>;
  metricLabel: string;
  selectedBanks: string[];
  onChange: (nextSelected: string[]) => void;
};

export function BankSelector({ banks, metricLabel, selectedBanks, onChange }: BankSelectorProps) {
  function toggleBank(institutionCode: string) {
    onChange(
      selectedBanks.includes(institutionCode)
        ? selectedBanks.filter((code) => code !== institutionCode)
        : [...selectedBanks, institutionCode]
    );
  }

  return (
    <aside className="rounded-3xl border border-border bg-panel p-6">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.24em] text-brand">Banks</p>
          <h3 className="mt-2 text-xl font-semibold text-white">Select or deselect</h3>
          <p className="mt-2 text-sm text-muted">Initial selection uses the top 5 banks for the active metric.</p>
        </div>
      </div>

      <div className="mt-4 flex gap-2">
        <button
          type="button"
          onClick={() => onChange(banks.map((bank) => bank.institutionCode))}
          className="rounded-full border border-border bg-panelMuted px-3 py-2 text-xs font-medium text-white transition hover:border-brand/50"
        >
          Select all
        </button>
        <button
          type="button"
          onClick={() => onChange([])}
          className="rounded-full border border-border bg-panelMuted px-3 py-2 text-xs font-medium text-white transition hover:border-brand/50"
        >
          Clear
        </button>
      </div>

      <div className="mt-6 space-y-3">
        {banks.map((bank) => {
          const checked = selectedBanks.includes(bank.institutionCode);

          return (
            <label
              key={bank.institutionCode}
              className="flex cursor-pointer items-start gap-3 rounded-2xl border border-border bg-panelMuted p-3"
            >
              <input
                type="checkbox"
                checked={checked}
                onChange={() => toggleBank(bank.institutionCode)}
                className="mt-1 h-4 w-4 rounded border-border bg-panelMuted text-brand focus:ring-brand"
              />
              <span className="min-w-0 flex-1">
                <span className="block truncate text-sm font-medium text-white">{bank.institutionName}</span>
                <span className="mt-1 block text-xs text-muted">
                  {metricLabel}: {bank.latestValue === null ? "—" : formatMoney(bank.latestValue)}
                </span>
              </span>
            </label>
          );
        })}
      </div>
    </aside>
  );
}
