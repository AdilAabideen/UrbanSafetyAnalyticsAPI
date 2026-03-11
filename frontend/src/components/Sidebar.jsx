function Sidebar() {
  return (
    <aside className="border-r border-white/5 bg-[#030b0e] p-4 py-2">
      <div className="flex h-[62px] items-center">
        <img
          className="h-auto w-[160px] object-contain"
          src="https://westyorkshire.firearmslicensing.uk/logo"
          alt="West Yorkshire Police logo"
        />
      </div>

      <div className="flex flex-col gap-2 py-4">
        <button
          className="flex items-center gap-2 w-full rounded-lg bg-cyan-100/10 px-3 py-2 text-left text-sm text-cyan-50 font-hind font-medium"
          type="button"
        >
          <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polygon points="1 6 1 22 8 18 16 22 23 18 23 2 16 6 8 2 1 6" />
            <line x1="8" y1="2" x2="8" y2="18" />
            <line x1="16" y1="6" x2="16" y2="22" />
          </svg>
          Data Map
        </button>
      </div>
    </aside>
  );
}

export default Sidebar;

