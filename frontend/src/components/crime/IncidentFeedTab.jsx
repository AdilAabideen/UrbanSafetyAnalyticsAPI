import EmptyAnalyticsState from "../shared/EmptyAnalyticsState";
import CrimeFeedRow from "./CrimeFeedRow";
import { formatCount } from "../../utils/formatters";

function IncidentFeedTab({
  crimeRows,
  hasNextPage,
  hasPreviousPage,
  pageNumber,
  selectedCrime,
  onNextPage,
  onPreviousPage,
  onSelectCrime,
  isLoading,
}) {
  if (isLoading) {
    return <EmptyAnalyticsState message="Loading incidents from `/crimes/incidents`." />;
  }

  if (!crimeRows.length) {
    return (
      <EmptyAnalyticsState message="No crimes match this filter set. Adjust the filters to repopulate the incident feed." />
    );
  }

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/5 px-4 py-3 text-sm text-cyan-100/70">
        <div>Paginated incident rows returned by `/crimes/incidents`.</div>

        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={onPreviousPage}
            disabled={!hasPreviousPage}
            className="rounded-full border border-white/10 px-3 py-1.5 text-xs uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-100/10 disabled:cursor-not-allowed disabled:opacity-35"
          >
            Previous
          </button>

          <span className="rounded-full border border-cyan-100/10 bg-cyan-100/5 px-3 py-1.5 text-xs uppercase tracking-[0.18em] text-cyan-100/65">
            Page {formatCount(pageNumber)}
          </span>

          <button
            type="button"
            onClick={onNextPage}
            disabled={!hasNextPage}
            className="rounded-full border border-white/10 px-3 py-1.5 text-xs uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-100/10 disabled:cursor-not-allowed disabled:opacity-35"
          >
            Next
          </button>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        <div className="divide-y divide-white/5">
          {crimeRows.map((crime) => (
            <CrimeFeedRow
              key={`${crime.recordId ?? crime.crimeId ?? crime.location}-${crime.month}`}
              crime={crime}
              isSelected={crime.recordId === selectedCrime?.recordId}
              onSelect={() => {
                onSelectCrime(crime);
              }}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

export default IncidentFeedTab;
