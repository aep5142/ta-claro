"use client";

import { useState } from "react";
import { formatMoney, formatMonthLabel } from "@/lib/formatters";

type MetricLineChartProps = {
  months: string[];
  series: Array<{
    institutionCode: string;
    institutionName: string;
    series: Record<string, number | null>;
  }>;
  metricType: "money" | "count";
};

const palette = ["#5eead4", "#60a5fa", "#f472b6", "#fbbf24", "#a78bfa", "#fb7185", "#34d399"];

export function MetricLineChart({ months, series, metricType }: MetricLineChartProps) {
  const width = 880;
  const height = 360;
  const padding = { top: 18, right: 18, bottom: 42, left: 56 };
  const [hoveredPoint, setHoveredPoint] = useState<{
    institutionName: string;
    month: string;
    value: number;
    color: string;
    x: number;
    y: number;
  } | null>(null);

  const allValues = series.flatMap((bank) =>
    months
      .map((month) => bank.series[month])
      .filter((value): value is number => typeof value === "number")
  );

  if (!allValues.length) {
    return null;
  }

  const minValue = Math.min(...allValues);
  const maxValue = Math.max(...allValues);
  const valueRange = maxValue - minValue || 1;

  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;

  const xForIndex = (index: number) =>
    padding.left + (months.length === 1 ? chartWidth / 2 : (chartWidth * index) / (months.length - 1));
  const yForValue = (value: number) =>
    padding.top + chartHeight - ((value - minValue) / valueRange) * chartHeight;

  const yTicks = Array.from({ length: 4 }, (_, index) => minValue + (valueRange * index) / 3).reverse();
  const tooltipValueLabel = metricType === "money" ? `${formatMoney(hoveredPoint?.value ?? 0)} CLP` : formatMoney(hoveredPoint?.value ?? 0);

  return (
    <div className="space-y-4">
      <div className="overflow-x-auto">
        <div className="relative min-w-[720px]">
          {hoveredPoint ? (
            <div
              className="pointer-events-none absolute z-10 min-w-44 rounded-2xl border border-border bg-[#07101c]/95 px-4 py-3 shadow-2xl"
              style={{
                left: `${Math.min(Math.max((hoveredPoint.x / width) * 100, 12), 82)}%`,
                top: `${Math.min(Math.max(((hoveredPoint.y - 72) / height) * 100, 4), 72)}%`,
                transform: "translate(-50%, -100%)",
              }}
            >
              <div className="flex items-center gap-2">
                <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: hoveredPoint.color }} />
                <span className="text-sm font-semibold text-white">{hoveredPoint.institutionName}</span>
              </div>
              <p className="mt-2 text-xs text-muted">{formatMonthLabel(hoveredPoint.month)}</p>
              <p className="mt-1 text-sm font-medium text-white">{tooltipValueLabel}</p>
            </div>
          ) : null}

          <svg viewBox={`0 0 ${width} ${height}`} className="min-w-[720px]">
            {yTicks.map((tick) => {
              const y = yForValue(tick);
              return (
                <g key={tick}>
                  <line x1={padding.left} y1={y} x2={width - padding.right} y2={y} stroke="#22344f" strokeDasharray="4 6" />
                  <text x={padding.left - 10} y={y + 4} textAnchor="end" fontSize="11" fill="#95a8c7">
                    {formatMoney(tick)}
                  </text>
                </g>
              );
            })}

            {months.map((month, index) => {
              const x = xForIndex(index);
              return (
                <g key={month}>
                  <line x1={x} y1={padding.top} x2={x} y2={height - padding.bottom} stroke="#16253c" />
                  <text x={x} y={height - 16} textAnchor="middle" fontSize="11" fill="#95a8c7">
                    {formatMonthLabel(month)}
                  </text>
                </g>
              );
            })}

            {series.map((bank, index) => {
              const color = palette[index % palette.length];
              const segments: string[] = [];
              let activeSegment = "";

              months.forEach((month, monthIndex) => {
                const value = bank.series[month];

                if (value === null || value === undefined) {
                  if (activeSegment) {
                    segments.push(activeSegment);
                    activeSegment = "";
                  }
                  return;
                }

                const x = xForIndex(monthIndex);
                const y = yForValue(value);
                activeSegment = `${activeSegment ? `${activeSegment} L` : "M"} ${x} ${y}`;
              });

              if (activeSegment) {
                segments.push(activeSegment);
              }

              return (
                <g key={bank.institutionCode}>
                  {segments.map((segment) => (
                    <path
                      key={`${bank.institutionCode}-${segment}`}
                      d={segment}
                      fill="none"
                      stroke={color}
                      strokeWidth="2.5"
                      strokeLinecap="round"
                    />
                  ))}
                  {months.map((month, monthIndex) => {
                    const value = bank.series[month];

                    if (value === null || value === undefined) {
                      return null;
                    }

                    const x = xForIndex(monthIndex);
                    const y = yForValue(value);

                    return (
                      <circle
                        key={`${bank.institutionCode}-${month}`}
                        cx={x}
                        cy={y}
                        r="5"
                        fill={color}
                        className="cursor-pointer transition hover:opacity-100 focus:opacity-100"
                        tabIndex={0}
                        onMouseEnter={() =>
                          setHoveredPoint({
                            institutionName: bank.institutionName,
                            month,
                            value,
                            color,
                            x,
                            y,
                          })
                        }
                        onMouseLeave={() => setHoveredPoint((current) => (current?.institutionName === bank.institutionName && current?.month === month ? null : current))}
                        onFocus={() =>
                          setHoveredPoint({
                            institutionName: bank.institutionName,
                            month,
                            value,
                            color,
                            x,
                            y,
                          })
                        }
                        onBlur={() => setHoveredPoint((current) => (current?.institutionName === bank.institutionName && current?.month === month ? null : current))}
                      />
                    );
                  })}
                </g>
              );
            })}
          </svg>
        </div>
      </div>

      <div className="flex flex-wrap gap-3">
        {series.map((bank, index) => (
          <div key={bank.institutionCode} className="flex items-center gap-2 rounded-full border border-border bg-panelMuted px-3 py-2 text-xs text-white">
            <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: palette[index % palette.length] }} />
            <span>{bank.institutionName}</span>
          </div>
        ))}
      </div>

      <p className="text-xs text-muted">
        {metricType === "money" ? "Money values are shown as rounded CLP integers." : "Transaction counts are shown as rounded integers."}
      </p>
    </div>
  );
}
