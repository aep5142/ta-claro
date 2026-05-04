"use client";

import { useState } from "react";
import { getBankColor } from "@/lib/bank-presentation";
import { formatDecimal, formatMoney, formatMoneyWithSymbol, formatMonthLabel, formatPercent } from "@/lib/formatters";

type MetricLineChartProps = {
  months: string[];
  systemMonthTotals: Record<string, number>;
  series: Array<{
    institutionCode: string;
    institutionName: string;
    series: Record<string, number | null>;
  }>;
  metricType: "money" | "count" | "decimal" | "ratio";
  showSystemShare?: boolean;
};

export function MetricLineChart({
  months,
  systemMonthTotals,
  series,
  metricType,
  showSystemShare = true,
}: MetricLineChartProps) {
  const width = 1120;
  const lineHeight = 360;
  const barRowHeight = 42;
  const barHeight = Math.max(180, series.length * barRowHeight + 70);
  const height = months.length === 1 ? barHeight : lineHeight;
  const [hoveredPoint, setHoveredPoint] = useState<{
    institutionName: string;
    month: string;
    value: number;
    color: string;
    x: number;
    y: number;
    systemShare: number | null;
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
  const rawRange = maxValue - minValue || 1;
  const barMaxValue = Math.max(...allValues, 1);

  const isSingleMonth = months.length === 1;
  const monthTotals = systemMonthTotals;

  const formatTickLabel = (tick: number) => {
    if (metricType === "money") {
      return formatMoneyWithSymbol(tick);
    }
    if (metricType === "ratio") {
      return formatPercent(tick);
    }
    if (metricType === "decimal") {
      return formatDecimal(tick);
    }
    return formatMoney(tick);
  };

  const lowerPadding = metricType === "decimal" ? Math.max(rawRange * 0.15, 0.05) : metricType === "ratio" ? Math.max(rawRange * 0.15, 2) : Math.max(rawRange * 0.12, 1);
  const upperPadding = metricType === "decimal" ? Math.max(rawRange * 0.1, 0.05) : metricType === "ratio" ? Math.max(rawRange * 0.1, 2) : Math.max(rawRange * 0.08, 1);
  const targetStep = Math.max((maxValue - minValue + lowerPadding + upperPadding) / 3, metricType === "decimal" ? 0.05 : metricType === "ratio" ? 2 : 1);
  const stepMagnitude = 10 ** Math.floor(Math.log10(targetStep));
  const stepFraction = targetStep / stepMagnitude;
  const niceStep =
    (stepFraction <= 1 ? 1 : stepFraction <= 2 ? 2 : stepFraction <= 5 ? 5 : 10) * stepMagnitude;
  const chartMaxValue = Math.ceil((maxValue + upperPadding) / niceStep) * niceStep;
  const chartMinValue = Math.max(0, Math.floor((minValue - lowerPadding) / niceStep) * niceStep);
  const chartRange = chartMaxValue - chartMinValue || 1;
  const yTickCount = Math.max(4, Math.ceil(chartRange / niceStep) + 1);
  const yTicks = Array.from({ length: yTickCount }, (_, index) => chartMinValue + niceStep * index).reverse();
  const maxTickLabelLength = Math.max(...yTicks.map((tick) => formatTickLabel(tick).length));
  const estimatedTickLabelWidth = maxTickLabelLength * 7;
  const multiMonthLeftPadding = Math.max(56, Math.min(160, estimatedTickLabelWidth + 24));
  const padding =
    months.length === 1
      ? { top: 20, right: 20, bottom: 24, left: 144 }
      : { top: 18, right: 110, bottom: 42, left: multiMonthLeftPadding };

  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;

  const xForIndex = (index: number) =>
    padding.left + (isSingleMonth ? chartWidth / 2 : (chartWidth * index) / (months.length - 1));
  const yForValue = (value: number) => padding.top + chartHeight - ((value - chartMinValue) / chartRange) * chartHeight;
  const pointRadius = months.length > 72 ? 2.2 : months.length > 36 ? 2.8 : months.length > 18 ? 3.4 : 4.2;
  const monthLabelStep =
    months.length > 96 ? 12 : months.length > 72 ? 6 : months.length > 36 ? 4 : months.length > 24 ? 3 : months.length > 12 ? 2 : 1;
  const singleMonthBanks =
    months.length === 1
      ? [...series]
          .map((bank) => ({
            ...bank,
            value: bank.series[months[0]],
          }))
          .sort((left, right) => {
            if (left.value === null || left.value === undefined) {
              return 1;
            }
            if (right.value === null || right.value === undefined) {
              return -1;
            }
            return right.value - left.value;
          })
      : [];
  const formatMetricValue = (value: number) => {
    if (metricType === "money") {
      return formatMoneyWithSymbol(value);
    }
    if (metricType === "ratio") {
      return formatPercent(value);
    }
    if (metricType === "decimal") {
      return formatDecimal(value);
    }
    return formatMoney(value);
  };
  const tooltipValueLabel = formatMetricValue(hoveredPoint?.value ?? 0);
  const tooltipShareLabel =
    hoveredPoint?.systemShare === null || hoveredPoint?.systemShare === undefined ? null : formatPercent(hoveredPoint.systemShare);
  return (
    <div className="space-y-4">
      <div className="relative w-full">
        {hoveredPoint ? (
          <div
            className="pointer-events-none absolute z-10 w-[min(18rem,80vw)] rounded-2xl border border-border bg-[#07101c]/95 px-3 py-2.5 shadow-2xl sm:min-w-44 sm:px-4 sm:py-3"
            style={{
              left: `${Math.min(Math.max((hoveredPoint.x / width) * 100, 8), 86)}%`,
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
            {tooltipShareLabel && showSystemShare ? (
              <p className="mt-1 text-xs italic text-brand">{tooltipShareLabel} of the system</p>
            ) : null}
          </div>
        ) : null}

        <svg viewBox={`0 0 ${width} ${height}`} className="h-auto w-full">
          {isSingleMonth ? (
            <>
              <text x={padding.left} y={padding.top - 2} textAnchor="start" fontSize="11" fill="#95a8c7">
                {formatMonthLabel(months[0])}
              </text>
              {singleMonthBanks.map((bank, index) => {
                const value = bank.value;
                if (value === null || value === undefined) {
                  return null;
                }

                const barY = padding.top + index * barRowHeight + 10;
                const barValueWidth = 120;
                const barWidth = (value / barMaxValue) * (chartWidth - barValueWidth);
                const color = getBankColor(bank.institutionCode);
                const labelX = padding.left + barWidth + 12;
                const totalValue = monthTotals[months[0]] || 0;
                const systemShare = totalValue > 0 ? (value / totalValue) * 100 : null;

                return (
                  <g key={bank.institutionCode}>
                    <text x={padding.left - 12} y={barY + 14} textAnchor="end" fontSize="11" fill="#d4def0">
                      {bank.institutionName}
                    </text>
                    <rect
                      x={padding.left}
                      y={barY}
                      width={Math.max(barWidth, 6)}
                      height={22}
                      rx={11}
                      fill={color}
                      className="cursor-pointer"
                      onMouseEnter={() =>
                        setHoveredPoint({
                          institutionName: bank.institutionName,
                          month: months[0],
                          value,
                          color,
                          x: padding.left + Math.max(barWidth, 6),
                          y: barY,
                          systemShare,
                        })
                      }
                      onMouseLeave={() => setHoveredPoint(null)}
                      onFocus={() =>
                        setHoveredPoint({
                          institutionName: bank.institutionName,
                          month: months[0],
                          value,
                          color,
                          x: padding.left + Math.max(barWidth, 6),
                          y: barY,
                          systemShare,
                        })
                      }
                      onBlur={() => setHoveredPoint(null)}
                      tabIndex={0}
                    />
                    <text x={labelX} y={barY + 14} textAnchor="start" fontSize="11" fill="#95a8c7">
                      {formatMetricValue(value)}
                    </text>
                  </g>
                );
              })}
            </>
          ) : null}

          {!isSingleMonth ? (
            <>
              {yTicks.map((tick) => {
                const y = yForValue(tick);
                return (
                  <g key={tick}>
                    <line x1={padding.left} y1={y} x2={width - padding.right} y2={y} stroke="#22344f" strokeDasharray="4 6" />
                    <text x={padding.left - 10} y={y + 4} textAnchor="end" fontSize="11" fill="#95a8c7">
                      {formatTickLabel(tick)}
                    </text>
                  </g>
                );
              })}

              {months.map((month, index) => {
                const x = xForIndex(index);
                const shouldLabel = index === 0 || index === months.length - 1 || index % monthLabelStep === 0;
                return (
                  <g key={month}>
                    <line x1={x} y1={padding.top} x2={x} y2={height - padding.bottom} stroke="#16253c" />
                    {shouldLabel ? (
                      <text x={x} y={height - 16} textAnchor="middle" fontSize="11" fill="#95a8c7">
                        {formatMonthLabel(month)}
                      </text>
                    ) : null}
                  </g>
                );
              })}

              {series.map((bank) => {
                const color = getBankColor(bank.institutionCode);
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
                          r={pointRadius}
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
                              systemShare: monthTotals[month] > 0 ? (value / monthTotals[month]) * 100 : null,
                            })
                          }
                          onMouseLeave={() =>
                            setHoveredPoint((current) =>
                              current?.institutionName === bank.institutionName && current?.month === month ? null : current
                            )
                          }
                          onFocus={() =>
                            setHoveredPoint({
                              institutionName: bank.institutionName,
                              month,
                              value,
                              color,
                              x,
                              y,
                              systemShare: monthTotals[month] > 0 ? (value / monthTotals[month]) * 100 : null,
                            })
                          }
                          onBlur={() =>
                            setHoveredPoint((current) =>
                              current?.institutionName === bank.institutionName && current?.month === month ? null : current
                            )
                          }
                        />
                      );
                    })}
                  </g>
                );
              })}
            </>
          ) : null}
        </svg>
      </div>

      <div className="flex flex-wrap gap-3">
        {series.map((bank) => (
          <div key={bank.institutionCode} className="flex items-center gap-2 rounded-full border border-border bg-panelMuted px-3 py-2 text-xs text-white">
            <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: getBankColor(bank.institutionCode) }} />
            <span>{bank.institutionName}</span>
          </div>
        ))}
      </div>

    </div>
  );
}
