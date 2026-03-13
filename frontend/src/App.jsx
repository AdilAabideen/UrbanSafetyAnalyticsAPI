import { Suspense, lazy, startTransition, useEffect, useMemo, useState } from "react";
import Sidebar from "./components/Sidebar";
import TopBar from "./components/TopBar";
import { config } from "./config/env";
import { authService } from "./services";

const DataMapPage = lazy(() => import("./pages/DataMapPage"));
const LoginPage = lazy(() => import("./pages/LoginPage"));
const ProfilePage = lazy(() => import("./pages/ProfilePage"));

const ROUTES = {
  map: "/",
  login: "/login",
  profile: "/profile",
};

const KNOWN_ROUTES = new Set(Object.values(ROUTES));

function normalizePathname(pathname) {
  const trimmedPath = pathname?.replace(/\/+$/, "") || "/";
  const nextPath = trimmedPath === "" ? "/" : trimmedPath;

  return KNOWN_ROUTES.has(nextPath) ? nextPath : ROUTES.map;
}

function App() {
  const [route, setRoute] = useState(() => normalizePathname(window.location.pathname));
  const [session, setSession] = useState(() => authService.getStoredSession());
  const [authReady, setAuthReady] = useState(() => !authService.getStoredSession().accessToken);
  const [loginNotice, setLoginNotice] = useState("");
  const docsUrl = useMemo(() => `${config.apiBaseUrl}/docs`, []);
  const isAuthenticated = Boolean(session.accessToken);

  function navigate(path, { replace = false } = {}) {
    const nextPath = normalizePathname(path);
    const historyMethod = replace ? "replaceState" : "pushState";

    if (window.location.pathname !== nextPath) {
      window.history[historyMethod]({}, "", nextPath);
    }

    startTransition(() => {
      setRoute(nextPath);
    });
  }

  function persistSession(nextSession) {
    setSession(nextSession);
    authService.storeSession(nextSession);
  }

  function clearSession() {
    setSession({ accessToken: "", user: null });
    authService.clearStoredSession();
  }

  async function refreshCurrentUser(accessToken = session.accessToken) {
    if (!accessToken) {
      throw new Error("No access token is available for this request.");
    }

    const response = await authService.getCurrentUser(accessToken);
    const nextSession = {
      accessToken,
      user: response.user,
    };

    persistSession(nextSession);
    return response.user;
  }

  async function handleLogin(credentials) {
    const response = await authService.login(credentials);
    persistSession({
      accessToken: response.access_token,
      user: response.user,
    });
    setLoginNotice("");
    navigate(ROUTES.map);
    return response;
  }

  async function handleRegister(credentials) {
    return authService.register(credentials);
  }

  async function handleProfileUpdate(updates) {
    const response = await authService.updateProfile(updates, session.accessToken);
    persistSession({
      accessToken: session.accessToken,
      user: response.user,
    });
    return response.user;
  }

  function handleLogout() {
    clearSession();
    setLoginNotice("Signed out successfully.");
    navigate(ROUTES.login, {
      replace: route === ROUTES.profile,
    });
  }

  useEffect(() => {
    function handlePopState() {
      startTransition(() => {
        setRoute(normalizePathname(window.location.pathname));
      });
    }

    window.addEventListener("popstate", handlePopState);

    return () => {
      window.removeEventListener("popstate", handlePopState);
    };
  }, []);

  useEffect(() => {
    const storedSession = authService.getStoredSession();

    if (!storedSession.accessToken) {
      return;
    }

    let cancelled = false;
    setAuthReady(false);

    authService
      .getCurrentUser(storedSession.accessToken)
      .then((response) => {
        if (cancelled) {
          return;
        }

        persistSession({
          accessToken: storedSession.accessToken,
          user: response.user,
        });

        if (normalizePathname(window.location.pathname) === ROUTES.login) {
          navigate(ROUTES.map, { replace: true });
        }
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }

        clearSession();
        setLoginNotice(
          error?.status === 401
            ? "Your token is missing, invalid, or expired. Sign in again."
            : error?.message || "Could not restore the saved session.",
        );
        navigate(ROUTES.login, { replace: true });
      })
      .finally(() => {
        if (!cancelled) {
          setAuthReady(true);
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!authReady) {
      return;
    }

    if (route === ROUTES.profile && !isAuthenticated) {
      navigate(ROUTES.login, { replace: true });
      return;
    }

    if (route === ROUTES.login && isAuthenticated) {
      navigate(ROUTES.map, { replace: true });
    }
  }, [authReady, isAuthenticated, route]);

  if (route === ROUTES.login) {
    return (
      <Suspense fallback={<RouteLoadingPanel label="Loading login" />}>
        <LoginPage
          apiBaseUrl={config.apiBaseUrl}
          notice={loginNotice}
          onLogin={handleLogin}
          onRegister={handleRegister}
        />
      </Suspense>
    );
  }

  if (route === ROUTES.profile) {
    return (
      <Suspense fallback={<RouteLoadingPanel label="Loading profile" />}>
        <ProfilePage
          apiBaseUrl={config.apiBaseUrl}
          loading={!authReady}
          user={session.user}
          onRefresh={refreshCurrentUser}
          onUpdateProfile={handleProfileUpdate}
          onLogout={handleLogout}
          onBackToMap={() => navigate(ROUTES.map)}
        />
      </Suspense>
    );
  }

  const navItems = [
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
  ];

  const authItem = isAuthenticated
    ? {
        id: "profile",
        label: "Profile",
        icon: (
          <>
            <path d="M20 21a8 8 0 0 0-16 0" />
            <circle cx="12" cy="7" r="4" />
          </>
        ),
      }
    : {
        id: "auth",
        label: "Login / Register",
        icon: (
          <>
            <rect x="4" y="11" width="16" height="10" rx="2" />
            <path d="M8 11V7a4 4 0 1 1 8 0v4" />
          </>
        ),
      };

  return (
    <div className="flex h-screen w-full">
      <Sidebar
        items={navItems}
        authItem={authItem}
        activeItemId="map"
        onSelectPage={(itemId) => {
          if (itemId === "map") {
            navigate(ROUTES.map);
          }

          if (itemId === "auth") {
            navigate(ROUTES.login);
          }

          if (itemId === "profile") {
            navigate(ROUTES.profile);
          }
        }}
      />

      <main className="flex min-h-0 flex-1 flex-col bg-[#071316]">
        <TopBar docsUrl={docsUrl} />
        <Suspense fallback={<RouteLoadingPanel label="Loading map" />}>
          <DataMapPage />
        </Suspense>
      </main>
    </div>
  );
}

function RouteLoadingPanel({ label }) {
  return (
    <div className="flex min-h-0 flex-1 items-center justify-center px-4 py-10">
      <div className="w-full max-w-lg rounded-[28px] border border-white/10 bg-[#021116]/85 p-8 text-center shadow-[var(--shadow-panel)]">
        <p className="text-xs font-semibold uppercase tracking-[0.28em] text-cyan-100/45">
          Route Load
        </p>
        <p className="font-display mt-3 text-3xl font-semibold text-[#f3fff9]">{label}</p>
      </div>
    </div>
  );
}

export default App;
