import { useState } from "react";
import { formatMonthLabel, formatCount } from "../../utils/formatters";

function TimeSeriesChart({ series }) {
  const [hoveredIndex, setHoveredIndex] = useState(null);
  const width = 720;
  const height = 260;
  const padding = 20;
  const maxCount = Math.max(...series.map((item) => item.count), 1);
  const points = series.map((item, index) => {
    const x =
      series.length === 1
        ? width / 2
        : padding + (index / (series.length - 1)) * (width - padding * 2);
    const y = height - padding - (item.count / maxCount) * (height - padding * 2);
    return { ...item, x, y };
  });
  const linePath = points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
    .join(" ");
  const areaPath = `${linePath} L ${points[points.length - 1].x} ${height - padding} L ${points[0].x} ${height - padding} Z`;

  const hovered = hoveredIndex !== null ? points[hoveredIndex] : null;

  return (
    <div className="relative">
      <svg viewBox={`0 0 ${width} ${height}`} className="h-[260px] w-full">
        {[0, 1, 2, 3].map((step) => {
          const y = padding + (step / 3) * (height - padding * 2);

          return (
            <line
              key={step}
              x1={padding}
              x2={width - padding}
              y1={y}
              y2={y}
              stroke="rgba(178, 245, 234, 0.12)"
              strokeWidth="1"
            />
          );
        })}

        <path d={areaPath} fill="rgba(34, 211, 238, 0.14)" />
        <path d={linePath} fill="none" stroke="#22d3ee" strokeWidth="3" strokeLinecap="round" />

        {points.map((point, index) => (
          <g
            key={`${point.month}-${index}`}
            onMouseEnter={() => setHoveredIndex(index)}
            onMouseLeave={() => setHoveredIndex(null)}
            className="cursor-pointer"
          >
            <circle cx={point.x} cy={point.y} r="14" fill="transparent" />
            <circle
              cx={point.x}
              cy={point.y}
              r={hoveredIndex === index ? 6 : 4}
              fill="#39ef7d"
              className="transition-all duration-150"
            />
          </g>
        ))}
      </svg>

      {hovered && (
        <div
          className="pointer-events-none absolute z-20 -translate-x-1/2 -translate-y-full rounded-xl border border-cyan-100/15 bg-[#030b0e]/95 px-3 py-2 text-xs shadow-lg backdrop-blur-sm"
          style={{
            left: `${(hovered.x / width) * 100}%`,
            top: `${(hovered.y / height) * 100}%`,
          }}
        >
          <p className="font-semibold text-cyan-50">{formatMonthLabel(hovered.month)}</p>
          <p className="mt-0.5 text-cyan-100/60">{formatCount(hovered.count)} incidents</p>
        </div>
      )}

      <div className="mt-3 grid gap-2 text-xs text-cyan-100/60 md:grid-cols-4">
        {series.map((item) => (
          <div
            key={item.month}
            className="rounded-xl border border-cyan-100/10 bg-cyan-100/5 px-3 py-2"
          >
            <p>{formatMonthLabel(item.month)}</p>
            <p className="mt-1 font-semibold text-cyan-50">{formatCount(item.count)}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

export default TimeSeriesChart;
