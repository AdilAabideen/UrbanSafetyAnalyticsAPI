import EmptyAnalyticsState from "../shared/EmptyAnalyticsState";
import RoadRiskRow from "./RoadRiskRow";
import { formatCount } from "../../utils/formatters";
import { RISK_SORT_OPTIONS } from "../../utils/roadUtils";

function RoadRiskTab({
  riskRows,
  riskMeta,
  selectedRoad,
  page,
  pageSize,
  sort,
  onChangePage,
  onChangeSort,
  onSelectRoad,
  isLoading,
}) {
  if (isLoading) {
    return <EmptyAnalyticsState message="Loading dangerous roads from `/roads/analytics/risk`." />;
  }

  if (!riskRows.length) {
    return (
      <EmptyAnalyticsState message="No road segments match the current filter set. Adjust the filters to repopulate the risk feed." />
    );
  }

  const totalPages = Math.max(1, Math.ceil(riskRows.length / pageSize));
  const currentPage = Math.min(Math.max(page, 1), totalPages);
  const pageStartIndex = (currentPage - 1) * pageSize;
  const pagedRows = riskRows.slice(pageStartIndex, pageStartIndex + pageSize);
  const visibleStart = pageStartIndex + 1;
  const visibleEnd = pageStartIndex + pagedRows.length;

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/5 px-4 py-3 text-sm text-cyan-100/70">
        <div>
          Ranked shortlist returned by `/roads/analytics/risk` using the selected sort.
        </div>

        <div className="flex flex-wrap gap-2">
          {RISK_SORT_OPTIONS.map((option) => (
            <button
              key={option.value}
              type="button"
              onClick={() => onChangeSort(option.value)}
              className={`rounded-full px-3 py-1.5 text-xs uppercase tracking-[0.18em] transition-colors ${
                sort === option.value
                  ? "bg-cyan-100/10 text-cyan-50"
                  : "bg-transparent text-cyan-100/55 hover:bg-cyan-100/5 hover:text-cyan-50"
              }`}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>

      <div className="border-b border-white/5 px-4 py-3 text-xs uppercase tracking-[0.18em] text-cyan-100/45">
        Showing {formatCount(visibleStart)}-{formatCount(visibleEnd)} of {formatCount(riskRows.length)} loaded rows · fetch limit {formatCount(riskMeta?.limit || riskRows.length)} · sorted by {sort.replaceAll("_", " ")}
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        <div className="divide-y divide-white/5">
          {pagedRows.map((road) => (
            <RoadRiskRow
              key={road.selectionKey}
              road={road}
              isSelected={road.selectionKey === selectedRoad?.selectionKey}
              onSelect={() => onSelectRoad(road)}
            />
          ))}
        </div>
      </div>

      {totalPages > 1 ? (
        <div className="flex flex-wrap items-center justify-between gap-3 border-t border-white/5 px-4 py-3">
          <p className="text-xs uppercase tracking-[0.18em] text-cyan-100/45">
            Page {formatCount(currentPage)} of {formatCount(totalPages)}
          </p>

          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={() => onChangePage(currentPage - 1)}
              disabled={currentPage === 1}
              className="rounded-full border border-white/10 px-3 py-1.5 text-xs uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-100/10 disabled:cursor-not-allowed disabled:opacity-35"
            >
              Previous
            </button>

            {Array.from({ length: totalPages }, (_, index) => index + 1).map((pageNumber) => (
              <button
                key={pageNumber}
                type="button"
                onClick={() => onChangePage(pageNumber)}
                className={`rounded-full px-3 py-1.5 text-xs uppercase tracking-[0.18em] transition-colors ${
                  currentPage === pageNumber
                    ? "bg-cyan-100/10 text-cyan-50"
                    : "bg-transparent text-cyan-100/55 hover:bg-cyan-100/5 hover:text-cyan-50"
                }`}
              >
                {pageNumber}
              </button>
            ))}

            <button
              type="button"
              onClick={() => onChangePage(currentPage + 1)}
              disabled={currentPage === totalPages}
              className="rounded-full border border-white/10 px-3 py-1.5 text-xs uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-100/10 disabled:cursor-not-allowed disabled:opacity-35"
            >
              Next
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

export default RoadRiskTab;
