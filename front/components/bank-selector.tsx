import { useMemo, useState } from "react";
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
  const [showMoreOptions, setShowMoreOptions] = useState(false);
  const visibleMobileBanks = useMemo(() => {
    const selectedSet = new Set(selectedBanks);
    const selectedOnly = banks.filter((bank) => selectedSet.has(bank.institutionCode));
    const unselected = banks.filter((bank) => !selectedSet.has(bank.institutionCode));
    const fillCount = Math.max(0, 5 - selectedOnly.length);
    return [...selectedOnly, ...unselected.slice(0, fillCount)];
  }, [banks, selectedBanks]);
  const hiddenMobileBanks = useMemo(() => {
    const selectedSet = new Set(selectedBanks);
    const visibleCodes = new Set(visibleMobileBanks.map((bank) => bank.institutionCode));
    return banks.filter((bank) => !selectedSet.has(bank.institutionCode) && !visibleCodes.has(bank.institutionCode));
  }, [banks, selectedBanks, visibleMobileBanks]);

  function toggleBank(institutionCode: string) {
    onChange(
      selectedBanks.includes(institutionCode)
        ? selectedBanks.filter((code) => code !== institutionCode)
        : [...selectedBanks, institutionCode]
    );
  }

  return (
    <section className="border-t border-border pt-8">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h3 className="text-2xl font-semibold tracking-tight text-white sm:text-3xl">Banks shown</h3>
          <p className="mt-2 text-xs text-muted sm:text-sm">
            Initial selection uses the top 5 banks for the active metric.
          </p>
        </div>
        <div className="flex items-center gap-4 text-xs text-muted sm:text-sm">
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

      <div className="mt-6 space-y-3 border-t border-border pt-6 sm:hidden">
        {visibleMobileBanks.map((bank) => {
          const checked = selectedBanks.includes(bank.institutionCode);

          return (
            <button
              key={bank.institutionCode}
              type="button"
              onClick={() => toggleBank(bank.institutionCode)}
              className="flex items-center gap-3 text-left text-sm text-white transition hover:text-brand"
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

        {hiddenMobileBanks.length ? (
          <button
            type="button"
            onClick={() => setShowMoreOptions((current) => !current)}
            className="text-left text-sm text-muted transition hover:text-white"
          >
            {showMoreOptions ? "Hide options" : "More options"}
          </button>
        ) : null}

        {showMoreOptions
          ? hiddenMobileBanks.map((bank) => {
              const checked = selectedBanks.includes(bank.institutionCode);

              return (
                <button
                  key={bank.institutionCode}
                  type="button"
                  onClick={() => toggleBank(bank.institutionCode)}
                  className="flex items-center gap-3 text-left text-sm text-white transition hover:text-brand"
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
            })
          : null}
      </div>

      <div className="mt-6 hidden gap-x-6 gap-y-3 border-t border-border pt-6 sm:grid sm:grid-cols-2 xl:grid-cols-4">
        {banks.map((bank) => {
          const checked = selectedBanks.includes(bank.institutionCode);

          return (
            <button
              key={bank.institutionCode}
              type="button"
              onClick={() => toggleBank(bank.institutionCode)}
              className="flex items-center gap-3 text-left text-sm text-white transition hover:text-brand sm:text-base"
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
