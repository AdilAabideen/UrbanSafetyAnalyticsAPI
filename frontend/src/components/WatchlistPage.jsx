import { useMemo, useState } from "react";
import TopBar from "./TopBar";
import WatchlistPolygonMap from "./watchlist/WatchlistPolygonMap";
import WatchlistField from "./watchlist/WatchlistField";
import WatchlistCrimeTypeMultiSelect from "./watchlist/WatchlistCrimeTypeMultiSelect";
import WatchlistModeSelect from "./watchlist/WatchlistModeSelect";
import { watchlistService } from "../services";
import {
  formatCoordinateInput,
  parseBboxFromForm,
  toCrimeTypePayloadValue,
  createDefaultWatchlistForm,
  WATCHLIST_CRIME_TYPE_OPTIONS,
  WATCHLIST_MODE_OPTIONS,
  monthValueToApiDate,
  monthValueToApiDateSecond,
} from "../utils/watchlistUtils";

function WatchlistPage({ docsUrl, accessToken, onWatchlistCreated }) {
  const [watchlistForm, setWatchlistForm] = useState(createDefaultWatchlistForm);
  const [polygonPoints, setPolygonPoints] = useState([]);
  const [polygonClosed, setPolygonClosed] = useState(false);
  const [creatingWatchlist, setCreatingWatchlist] = useState(false);
  const [watchlistErrorMessage, setWatchlistErrorMessage] = useState("");
  const parsedBbox = useMemo(() => parseBboxFromForm(watchlistForm), [watchlistForm]);
  const hasValidMonthRange =
    Boolean(watchlistForm.startMonth) &&
    Boolean(watchlistForm.endMonth) &&
    watchlistForm.startMonth <= watchlistForm.endMonth;
  const isFormComplete = useMemo(
    () => [watchlistForm.name, watchlistForm.mode].every((value) => String(value).trim().length > 0),
    [watchlistForm],
  );
  const canProceed =
    isFormComplete &&
    hasValidMonthRange &&
    Boolean(parsedBbox) &&
    polygonClosed &&
    !creatingWatchlist;

  const handleFieldChange = (key, value) => {
    setWatchlistErrorMessage("");
    setWatchlistForm((current) => ({
      ...current,
      [key]: value,
    }));
  };

  const clearPolygon = () => {
    setPolygonPoints([]);
    setPolygonClosed(false);
    setWatchlistForm((current) => ({
      ...current,
      minLon: "",
      minLat: "",
      maxLon: "",
      maxLat: "",
    }));
  };

  const handlePolygonStart = () => {
    clearPolygon();
  };

  const handlePolygonDraft = (points) => {
    setPolygonPoints(points);
    setPolygonClosed(false);
  };

  const handlePolygonComplete = (points, bbox) => {
    setPolygonPoints(points);
    setPolygonClosed(true);
    setWatchlistForm((current) => ({
      ...current,
      minLon: formatCoordinateInput(bbox.minLon),
      minLat: formatCoordinateInput(bbox.minLat),
      maxLon: formatCoordinateInput(bbox.maxLon),
      maxLat: formatCoordinateInput(bbox.maxLat),
    }));
  };

  const handleCreateWatchlist = async () => {
    if (!canProceed || !parsedBbox) {
      return;
    }

    if (!accessToken) {
      setWatchlistErrorMessage("You need to sign in before creating watchlists.");
      return;
    }

    const payload = {
      name: watchlistForm.name.trim(),
      min_lon: parsedBbox.minLon,
      min_lat: parsedBbox.minLat,
      max_lon: parsedBbox.maxLon,
      max_lat: parsedBbox.maxLat,
      preference: {
        start_month: monthValueToApiDateSecond(watchlistForm.startMonth),
        end_month: monthValueToApiDateSecond(watchlistForm.endMonth),
        crime_types: watchlistForm.crimeTypes.map(toCrimeTypePayloadValue).filter(Boolean),
        travel_mode: watchlistForm.mode.toLowerCase() === "driving" ? "drive" : "walk",
      }
    };

    setCreatingWatchlist(true);
    setWatchlistErrorMessage("");

    try {
      const createdWatchlist = await watchlistService.createWatchlist(payload, accessToken);
      onWatchlistCreated?.(createdWatchlist);
    } catch (error) {
      setWatchlistErrorMessage(error?.message || "Failed to create the watchlist.");
    } finally {
      setCreatingWatchlist(false);
    }
  };

  return (
    <div className="flex min-h-0 flex-1 flex-col bg-[#071316]">
      <TopBar
        docsUrl={docsUrl}
        title="Watchlists"
        subtitle="Create a watchlist with bbox and preference payload matching the API schema."
      />

      <div className="min-h-0 flex-1 p-4">
        <div className="grid h-full min-h-0 grid-cols-2 grid-rows-1 overflow-hidden rounded-[26px] border border-white/5">
          <section className="flex h-full min-h-0 flex-col border-r-2 border-r-white/5">
            <div className="border-b border-white/5 px-5 py-4">
              <p className="text-[11px] uppercase tracking-[0.35em] text-cyan-100/40">Watchlist Setup</p>
              <h2 className="mt-2 text-xl font-semibold text-cyan-50">Create Watchlist</h2>

            </div>

            <div className="min-h-0 flex-1 overflow-y-auto px-3 py-0">
              <div className="space-y-3 rounded-[24px] p-4">
                <WatchlistField
                  label="NAME"
                  value={watchlistForm.name}
                  placeholder="Leeds Centre"
                  onChange={(value) => handleFieldChange("name", value)}
                />

                <div className="grid gap-3 md:grid-cols-2">
                  <WatchlistField
                    label="START MONTH"
                    type="month"
                    value={watchlistForm.startMonth}
                    onChange={(value) => handleFieldChange("startMonth", value)}
                  />
                  <WatchlistField
                    label="END MONTH"
                    type="month"
                    value={watchlistForm.endMonth}
                    onChange={(value) => handleFieldChange("endMonth", value)}
                  />
                </div>

                <WatchlistCrimeTypeMultiSelect
                  label="CRIME TYPE"
                  values={watchlistForm.crimeTypes}
                  options={WATCHLIST_CRIME_TYPE_OPTIONS}
                  onChange={(value) => handleFieldChange("crimeTypes", value)}
                />

                <WatchlistModeSelect
                  label="MODE"
                  value={watchlistForm.mode}
                  options={WATCHLIST_MODE_OPTIONS}
                  onChange={(value) => handleFieldChange("mode", value)}
                />

                <WatchlistField
                  label="MIN LONGITUDE"
                  value={watchlistForm.minLon}
                  placeholder="-1.620000"
                  inputMode="decimal"
                  onChange={(value) => handleFieldChange("minLon", value)}
                />
                <WatchlistField
                  label="MIN LATITUDE"
                  value={watchlistForm.minLat}
                  placeholder="53.780000"
                  inputMode="decimal"
                  onChange={(value) => handleFieldChange("minLat", value)}
                />
                <WatchlistField
                  label="MAX LONGITUDE"
                  value={watchlistForm.maxLon}
                  placeholder="-1.500000"
                  inputMode="decimal"
                  onChange={(value) => handleFieldChange("maxLon", value)}
                />
                <WatchlistField
                  label="MAX LATITUDE"
                  value={watchlistForm.maxLat}
                  placeholder="53.840000"
                  inputMode="decimal"
                  onChange={(value) => handleFieldChange("maxLat", value)}
                />

                {!hasValidMonthRange ? (
                  <div className="rounded-[16px] border border-amber-300/30 bg-amber-950/60 px-4 py-3 text-sm text-amber-100">
                    Start month must be before or equal to end month.
                  </div>
                ) : null}

                {watchlistErrorMessage ? (
                  <div className="rounded-[16px] border border-red-300/30 bg-[#4a0f0fd0] px-4 py-3 text-sm text-red-100">
                    {watchlistErrorMessage}
                  </div>
                ) : null}
              </div>
            </div>

            <div className="border-t border-white/5 px-5 py-4">
              <p className="text-sm text-cyan-100/55">
                {!accessToken
                  ? "Log in to create and store watchlists."
                  : canProceed
                    ? "Payload is valid and ready to create."
                    : "Complete required fields and polygon area to create this watchlist."}
              </p>

              <button
                type="button"
                onClick={handleCreateWatchlist}
                disabled={!canProceed || !accessToken}
                className="mt-4 w-full rounded-[16px] bg-cyan-50 px-4 py-3 text-sm font-semibold uppercase tracking-[0.18em] text-[#021116] transition-colors hover:bg-white disabled:cursor-not-allowed disabled:bg-cyan-100/20 disabled:text-cyan-100/40"
              >
                {creatingWatchlist ? "Creating..." : "Create Watchlist"}
              </button>
            </div>
          </section>

          <WatchlistPolygonMap
            polygonPoints={polygonPoints}
            polygonClosed={polygonClosed}
            onStartPolygon={handlePolygonStart}
            onPolygonDraft={handlePolygonDraft}
            onPolygonComplete={handlePolygonComplete}
            onClearPolygon={clearPolygon}
          />
        </div>
      </div>
    </div>
  );
}

export default WatchlistPage;
