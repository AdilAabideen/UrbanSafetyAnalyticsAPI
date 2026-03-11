function TopBar({ docsUrl }) {
  return (
    <header className="flex items-center justify-between gap-3 border-b border-darkTeal/10 bg-darkTeal p-4">
      <div>
        <h1 className="m-0 text-[28px] font-semibold leading-none tracking-[0.1px] text-cyan-50">
          Data Map
        </h1>
        <p className="mt-1 text-xs text-cyan-100">
          View all roads, crimes and footfall in regions in West Yorkshire
        </p>
      </div>

      <a
        className="rounded-full border border-cyan-100/25 bg-cyan-100/10 px-3 py-1.5 text-xs text-cyan-50 no-underline"
        href={docsUrl}
        target="_blank"
        rel="noreferrer"
      >
        Documentation
      </a>
    </header>
  );
}

export default TopBar;

