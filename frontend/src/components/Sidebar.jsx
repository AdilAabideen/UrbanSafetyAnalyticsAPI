const NAV_ITEMS = [
  {
    id: "map",
    label: "Data Map",
    icon: (
      <>
        <polygon points="1 6 1 22 8 18 16 22 23 18 23 2 16 6 8 2 1 6" />
        <line x1="8" y1="2" x2="8" y2="18" />
        <line x1="16" y1="6" x2="16" y2="22" />
      </>
    ),
  },
  {
    id: "crime",
    label: "Crime Page",
    icon: (
      <>
        <path d="M4 20h16" />
        <path d="M6 20V8l6-4 6 4v12" />
        <path d="M10 12h4" />
        <path d="M10 16h4" />
      </>
    ),
  },
];

function Sidebar({ activePage, onSelectPage }) {
  return (
    <aside className="flex shrink-0 flex-col border-r border-white/5 bg-[#030b0e] p-4 py-2">
      <div className="flex h-[62px] items-center">
        <img
          className="h-auto w-[160px] object-contain"
          src="https://westyorkshire.firearmslicensing.uk/logo"
          alt="West Yorkshire Police logo"
        />
      </div>

      <div className="flex flex-col gap-2 py-4">
        {NAV_ITEMS.map((item) => {
          const isActive = activePage === item.id;

          return (
            <button
              key={item.id}
              className={`flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-sm font-hind font-medium transition-colors ${
                isActive
                  ? "bg-cyan-100/10 text-cyan-50"
                  : "text-cyan-100/60 hover:bg-cyan-100/5 hover:text-cyan-50"
              }`}
              type="button"
              onClick={() => onSelectPage(item.id)}
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-4 w-4 shrink-0"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                {item.icon}
              </svg>
              {item.label}
            </button>
          );
        })}
      </div>
    </aside>
  );
}

export default Sidebar;
