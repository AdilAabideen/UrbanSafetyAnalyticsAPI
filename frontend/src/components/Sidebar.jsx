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
    id: "report-crime",
    label: "Report Crime",
    icon: (
      <>
        <path d="M12 3l8 4v5c0 5.5-4 8.8-8 9.8-4-1-8-4.3-8-9.8V7l8-4z" />
        <path d="M12 8v8" />
        <path d="M8 12h8" />
      </>
    ),
  },
  {
    id: "report-collision",
    label: "Report Collision",
    icon: (
      <>
        <circle cx="8" cy="17" r="3" />
        <circle cx="16" cy="17" r="3" />
        <path d="M5 17V9l3-3h5l3 4h3v7" />
        <path d="M12 6v6" />
      </>
    ),
  },
  {
    id: "view-reports",
    label: "View Reports",
    icon: (
      <>
        <path d="M8 6h11" />
        <path d="M8 12h11" />
        <path d="M8 18h11" />
        <path d="M3 6h.01" />
        <path d="M3 12h.01" />
        <path d="M3 18h.01" />
      </>
    ),
  },
  {
    id: "watchlist",
    label: "Create Watchlist",
    icon: (
      <>
        <path d="M12 3l7 4v5c0 5-3.5 8.5-7 9-3.5-.5-7-4-7-9V7l7-4z" />
        <path d="M9.5 12l1.8 1.8L14.8 10" />
      </>
    ),
  },
  {
    id: "view-watchlist",
    label: "View Watchlist",
    icon: (
      <>
        <path d="M2.5 12s3.5-6 9.5-6 9.5 6 9.5 6-3.5 6-9.5 6-9.5-6-9.5-6z" />
        <circle cx="12" cy="12" r="3" />
      </>
    ),
  },
];

const RESTRICTED_NAV_IDS = new Set(["view-reports", "watchlist", "view-watchlist"]);

function Sidebar({ activePage, isAdmin, isLoggedIn, onSelectPage, onLogout }) {
  const visibleNavItems = NAV_ITEMS.filter(
    (item) => isLoggedIn || !RESTRICTED_NAV_IDS.has(item.id),
  );

  return (
    <aside className="relative flex shrink-0 flex-col border-r border-white/5 bg-[#030b0e] p-4 py-2">
      <div className="flex h-[62px] items-center">
        <img
          className="h-auto w-[160px] object-contain"
          src="https://westyorkshire.firearmslicensing.uk/logo"
          alt="West Yorkshire Police logo"
        />
      </div>

      <div className="flex flex-col gap-2 py-4">
        {visibleNavItems.map((item) => {
          const isActive = activePage === item.id;

          return (
            <button
              key={item.id}
              className={`flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-sm font-hind font-medium transition-colors ${isActive
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
        {isLoggedIn ? (
          <>
            {isAdmin ? (
              <button
                type="button"
                onClick={() => onSelectPage("admin-approvals")}
                className={`flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-sm font-hind font-medium transition-colors ${
                  activePage === "admin-approvals"
                    ? "bg-cyan-100/10 text-cyan-50"
                    : "text-cyan-100/60 hover:bg-cyan-100/5 hover:text-cyan-50"
                }`}
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
                  <path d="M12 3l7 4v5c0 5-3.5 8.5-7 9-3.5-.5-7-4-7-9V7l7-4z" />
                  <path d="M9 12l2 2 4-4" />
                </svg>
                Admin Approvals
              </button>
            ) : null}

            <button
              type="button"
              onClick={() => onSelectPage("profile")}
              className={`flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-sm font-hind font-medium transition-colors ${
                activePage === "profile"
                  ? "bg-cyan-100/10 text-cyan-50"
                  : "text-cyan-100/60 hover:bg-cyan-100/5 hover:text-cyan-50"
              }`}
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
                <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
                <circle cx="12" cy="7" r="4" />
              </svg>
              Profile
            </button>
          </>
        ) : (
          <button
            type="button"
            onClick={() => window.location.href = "/login"}
            className="flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-sm font-hind font-medium text-cyan-100/60 transition-colors hover:bg-cyan-100/5 hover:text-cyan-50"
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
              <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
              <circle cx="12" cy="7" r="4" />
            </svg>
            Login / Signup
          </button>
        )}
      </div>

      {!isLoggedIn ? (
        <div className="absolute bottom-3 right-3 max-w-[230px] rounded-md border border-cyan-200/20 bg-cyan-100/5 px-2 py-1 text-right text-[11px] leading-tight text-cyan-100/70">
          You have to be logged in to use Advanced Analytical APIs
        </div>
      ) : null}
    </aside>
  );
}

export default Sidebar;
