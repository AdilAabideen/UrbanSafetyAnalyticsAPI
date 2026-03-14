function RoadInsightStrip({ insights }) {
  return (
    <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
      {insights.map((insight) => (
        <div
          key={insight}
          className="rounded-[18px] border border-cyan-100/10 bg-cyan-100/5 px-4 py-3 text-sm text-cyan-100/75"
        >
          {insight}
        </div>
      ))}
    </div>
  );
}

export default RoadInsightStrip;
