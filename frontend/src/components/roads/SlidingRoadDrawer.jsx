import RoadDetailField from "./RoadDetailField";
import { formatCount, formatDistanceKm, formatMetricValue, formatPercent, formatSignedPercent, formatBandLabel } from "../../utils/formatters";

function SlidingRoadDrawer({ road, onClose }) {
  return (
    <div className="pointer-events-none absolute inset-0 z-30 overflow-hidden">
      <button
        type="button"
        aria-label="Close roads drawer"
        onClick={onClose}
        className={`absolute inset-0 bg-black/45 transition-opacity duration-300 ${
          road ? "pointer-events-auto opacity-100" : "opacity-0"
        }`}
      />

      <div
        className={`absolute inset-y-0 right-0 w-full border-l border-white/10 bg-[#030b0e] shadow-2xl transition-transform duration-300 sm:w-[46vw] sm:max-w-[46vw] ${
          road ? "translate-x-0" : "translate-x-full"
        }`}
      >
        <div className="h-full overflow-y-auto p-4">
          {road ? (
            <div className="grid gap-4 grid-cols-1 grid-rows-2">
              <div className="space-y-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
                      Road Details
                    </p>
                    <h2 className="mt-2 text-2xl font-semibold text-cyan-50">
                      {road.name || road.highway}
                    </h2>
                    <p className="mt-1 text-sm text-cyan-100/60">
                      Selected from `{road.sourceType === "highway" ? "/roads/analytics/charts" : "/roads/analytics/risk"}`
                    </p>
                  </div>

                  <button
                    type="button"
                    onClick={onClose}
                    className="flex h-9 w-9 items-center justify-center rounded-lg border border-cyan-100/10 text-cyan-100/60 transition-colors hover:bg-cyan-100/10 hover:text-cyan-50"
                  >
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      className="h-4 w-4"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    >
                      <line x1="18" y1="6" x2="6" y2="18" />
                      <line x1="6" y1="6" x2="18" y2="18" />
                    </svg>
                  </button>
                </div>

                <div className="flex w-full justify-between gap-6 p-2">
                  <section className="w-full rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
                    <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
                      Segment Identity
                    </p>
                    <div className="mt-4 grid gap-4 md:grid-cols-2">
                      <RoadDetailField label="Road name" value={road.name || "Not supplied"} />
                      <RoadDetailField label="Highway" value={road.highway || "Unclassified"} />
                      <RoadDetailField label="Segment ID" value={road.roadId || "Not supplied"} subtle />
                      <RoadDetailField
                        label="Length"
                        value={formatDistanceKm(road.lengthKm)}
                      />
                      <RoadDetailField
                        label="Source type"
                        value={road.sourceType === "highway" ? "Charts highway group" : "Risk feed row"}
                      />
                      <RoadDetailField
                        label="Message"
                        value={road.message || "No narrative supplied"}
                        subtle
                      />
                    </div>
                  </section>

                  <section className="w-full rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
                    <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
                      Risk Metrics
                    </p>
                    <div className="mt-4 grid gap-4 md:grid-cols-2">
                      <RoadDetailField label="Incidents" value={formatCount(road.incidents)} />
                      <RoadDetailField
                        label="Incidents per km"
                        value={formatMetricValue(road.incidentsPerKm)}
                      />
                      <RoadDetailField label="Risk band" value={formatBandLabel(road.riskBand)} />
                      <RoadDetailField label="Score" value={formatMetricValue(road.score)} />
                      <RoadDetailField
                        label="Share of incidents"
                        value={formatPercent(road.shareOfIncidents)}
                      />
                      <RoadDetailField
                        label="Previous change"
                        value={formatSignedPercent(road.previousPeriodChangePct)}
                      />
                    </div>
                  </section>
                </div>
              </div>

              <section className="flex min-h-[320px] flex-col rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
                <div className="mb-3 flex items-center justify-between gap-3">
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
                      Selection Signals
                    </p>
                    <p className="mt-1 text-sm text-cyan-100/60">
                      This drawer now stays inside the analytics contract and does not fetch extra road geometry.
                    </p>
                  </div>
                </div>

                <div className="grid gap-4 md:grid-cols-2">
                  <div className="rounded-[18px] border border-white/5 bg-[#030b0e]/60 p-4">
                    <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/40">Dominant Crime</p>
                    <p className="mt-3 text-base font-semibold text-cyan-50">
                      {road.dominantCrimeType || "No dominant crime"}
                    </p>
                  </div>

                  <div className="rounded-[18px] border border-white/5 bg-[#030b0e]/60 p-4">
                    <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/40">Dominant Outcome</p>
                    <p className="mt-3 text-base font-semibold text-cyan-50">
                      {road.dominantOutcome || "No dominant outcome"}
                    </p>
                  </div>

                  <div className="rounded-[18px] border border-white/5 bg-[#030b0e]/60 p-4 md:col-span-2">
                    <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/40">Narrative</p>
                    <p className="mt-3 text-sm leading-6 text-cyan-100/70">
                      {road.message || "No narrative message is available for this row."}
                    </p>
                  </div>
                </div>
              </section>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

export default SlidingRoadDrawer;
