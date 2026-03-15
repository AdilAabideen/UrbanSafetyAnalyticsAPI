import { formatCount } from "../../utils/formatters";

function BandBreakdownRow({ item, maxValue }) {
  const widthPercent = maxValue > 0 ? (item.count / maxValue) * 100 : 0;

  return (
    <div>
      <div className="mb-2 flex items-center justify-between gap-3">
        <span className="text-sm text-cyan-100/75">{item.label}</span>
        <span className="text-sm font-semibold text-cyan-50">{formatCount(item.count)}</span>
      </div>
      <div className="h-2 overflow-hidden rounded-full bg-cyan-100/10">
        <div
          className={`h-full rounded-full ${item.fillClass}`}
          style={{ width: `${widthPercent}%` }}
        />
      </div>
    </div>
  );
}

export default BandBreakdownRow;
