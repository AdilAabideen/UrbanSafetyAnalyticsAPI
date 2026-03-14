import InfoComponents from "../InfoComponents";
import CrimeLocationMap from "./CrimeLocationMap";

function SlidingCrimeDrawer({ crime, isLoadingDetail, detailErrorMessage, onClose }) {
  return (
    <div className="pointer-events-none absolute inset-0 z-30 overflow-hidden">
      <button
        type="button"
        aria-label="Close crime drawer"
        onClick={onClose}
        className={`absolute inset-0 bg-black/45 transition-opacity duration-300 ${
          crime ? "pointer-events-auto opacity-100" : "opacity-0"
        }`}
      />

      <div
        className={`absolute inset-y-0 right-0 w-full border-l border-white/10 bg-[#030b0e] shadow-2xl transition-transform duration-300 sm:w-[60vw] sm:max-w-[60vw] ${
          crime ? "translate-x-0" : "translate-x-full"
        }`}
      >
        <div className="h-full overflow-y-auto p-4">
          {crime ? (
            <div className="grid h-full gap-4 lg:grid-cols-[minmax(260px,0.82fr),minmax(0,1.18fr)]">
              <div className="flex min-h-0 flex-col gap-3 overflow-y-auto">
                {detailErrorMessage ? (
                  <div className="rounded-xl border border-red-300/30 bg-[#480000b8] px-3 py-3 text-sm text-red-100">
                    {detailErrorMessage}
                  </div>
                ) : null}
                {isLoadingDetail ? (
                  <div className="rounded-xl border border-cyan-100/10 bg-cyan-100/5 px-3 py-3 text-sm text-cyan-100/80">
                    Loading crime GeoJSON...
                  </div>
                ) : null}

                <InfoComponents
                  compact
                  className="h-full max-w-none rounded-[20px] bg-[#071316]/70 shadow-none"
                  showActionButton={false}
                  recordId={crime.recordId}
                  crimeId={crime.crimeId}
                  crimeType={crime.crimeType}
                  month={crime.month}
                  reportedBy={crime.reportedBy}
                  fallsWithin={crime.fallsWithin}
                  location={crime.location}
                  lsoaCode={crime.lsoaCode}
                  lsoaName={crime.lsoaName}
                  outcomeCategory={crime.outcomeCategory}
                  context={crime.context}
                  onClose={onClose}
                />
              </div>

              <CrimeLocationMap crime={crime} isLoadingDetail={isLoadingDetail} />
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

export default SlidingCrimeDrawer;
