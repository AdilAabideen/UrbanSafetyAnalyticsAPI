function Sidebar({ items, authItem, activeItemId, onSelectPage }) {
  return (
    <aside className="flex min-h-0 w-[240px] shrink-0 flex-col border-r border-white/5 bg-[#030b0e] p-4 py-2">
      <div className="flex h-[62px] items-center">
        <img
          className="h-auto w-[160px] object-contain"
          src="https://westyorkshire.firearmslicensing.uk/logo"
          alt="West Yorkshire Police logo"
        />
      </div>

      <div className="flex flex-col gap-2 py-4">
        {items.map((item) => {
          const isActive = activeItemId === item.id;

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

      <div className="mt-auto flex flex-col gap-2 py-4">
        {authItem ? (
          <button
            key={authItem.id}
            className={`flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-sm font-hind font-medium transition-colors ${
              activeItemId === authItem.id
                ? "bg-cyan-100/10 text-cyan-50"
                : "text-cyan-100/60 hover:bg-cyan-100/5 hover:text-cyan-50"
            }`}
            type="button"
            onClick={() => onSelectPage(authItem.id)}
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
              {authItem.icon}
            </svg>
            {authItem.label}
          </button>
        ) : null}
      </div>
    </aside>
  );
}

export default Sidebar;
