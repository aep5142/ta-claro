import { getBankColor, getBankDisplayName } from "@/lib/bank-presentation";
import { cn } from "@/lib/utils";

type BankSelectorProps = {
  banks: Array<{
    institutionCode: string;
    institutionName: string;
  }>;
  selectedBanks: string[];
  onChange: (nextSelected: string[]) => void;
  onReset: () => void;
};

export function BankSelector({ banks, selectedBanks, onChange, onReset }: BankSelectorProps) {
  function toggleBank(institutionCode: string) {
    onChange(
      selectedBanks.includes(institutionCode)
        ? selectedBanks.filter((code) => code !== institutionCode)
        : [...selectedBanks, institutionCode]
    );
  }

  return (
    <section className="border-t border-border pt-8">
      <div className="flex items-end justify-between gap-4">
        <div>
          <h3 className="text-3xl font-semibold tracking-tight text-white">Banks shown</h3>
          <p className="mt-2 text-sm text-muted">
            Initial selection uses the top 5 banks for the active metric.
          </p>
        </div>
        <div className="flex items-center gap-4 text-sm text-muted">
          <button type="button" onClick={() => onChange(banks.map((bank) => bank.institutionCode))} className="transition hover:text-white">
            All
          </button>
          <button type="button" onClick={() => onChange([])} className="transition hover:text-white">
            None
          </button>
          <button type="button" onClick={onReset} className="transition hover:text-white">
            Reset
          </button>
        </div>
      </div>

      <div className="mt-6 grid gap-x-10 gap-y-4 border-t border-border pt-6 sm:grid-cols-2 xl:grid-cols-4">
        {banks.map((bank) => {
          const checked = selectedBanks.includes(bank.institutionCode);

          return (
            <button
              key={bank.institutionCode}
              type="button"
              onClick={() => toggleBank(bank.institutionCode)}
              className="flex items-center gap-3 text-left text-base text-white transition hover:text-brand"
            >
              <span
                className={cn(
                  "h-3.5 w-3.5 rounded-full border transition",
                  checked ? "border-transparent" : "border-muted/50 bg-transparent"
                )}
                style={{ backgroundColor: checked ? getBankColor(bank.institutionCode) : "transparent" }}
              />
              <span className="truncate">{getBankDisplayName(bank.institutionName)}</span>
            </button>
          );
        })}
      </div>
    </section>
  );
}
