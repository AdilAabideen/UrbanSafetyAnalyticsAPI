import { useCallback, useEffect, useMemo, useState } from "react";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import CollisionsPage from "./components/CollisionsPage";
import CrimePage from "./components/CrimePage";
import MapPage from "./components/MapPage";
import RoadsPage from "./components/RoadsPage";
import Sidebar from "./components/Sidebar";
import ViewWatchlistPage from "./components/ViewWatchlistPage";
import WatchlistPage from "./components/WatchlistPage";
import { config } from "./config/env";
import LoginPage from "./pages/LoginPage";
import ProfilePage from "./pages/ProfilePage";
import { authService } from "./services";

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<LoginRoute />} />
        <Route path="/*" element={<DashboardRoute />} />
      </Routes>
    </BrowserRouter>
  );
}

function LoginRoute() {
  const handleLogin = async (credentials) => {
    const result = await authService.login(credentials);
    const nextSession = {
      accessToken: result.access_token || result.accessToken || result.token || "",
      user: result.user || null,
    };
    authService.storeSession(nextSession);
    window.location.href = "/";
  };

  const handleRegister = async (credentials) => {
    await authService.register(credentials);
  };

  return (
    <LoginPage
      onLogin={handleLogin}
      onRegister={handleRegister}
    />
  );
}

function DashboardRoute() {
  const [activePage, setActivePage] = useState("map");
  const [selectedWatchlistId, setSelectedWatchlistId] = useState(null);
  const [session, setSession] = useState(() => authService.getStoredSession());
  const [user, setUser] = useState(session.user);
  const [loadingUser, setLoadingUser] = useState(false);
  const docsUrl = useMemo(() => `${config.apiBaseUrl}/docs`, []);
  const isLoggedIn = Boolean(session.accessToken);

  useEffect(() => {
    if (!session.accessToken) return;

    let isActive = true;

    const loadCurrentUser = async () => {
      setLoadingUser(true);

      try {
        const fetchedUser = await authService.getCurrentUser(session.accessToken);

        if (isActive) {
          setUser(fetchedUser);
        }
      } catch {
        if (isActive) {
          setUser(null);
        }
      } finally {
        if (isActive) {
          setLoadingUser(false);
        }
      }
    };

    void loadCurrentUser();

    return () => {
      isActive = false;
    };
  }, [session.accessToken]);

  const handleLogout = useCallback(() => {
    authService.clearStoredSession();
    setSession({ accessToken: "", user: null });
    setUser(null);
    window.location.href = "/login";
  }, []);

  const handleRefreshProfile = useCallback(async () => {
    const fetchedUser = await authService.getCurrentUser(session.accessToken);
    setUser(fetchedUser);
  }, [session.accessToken]);

  const handleUpdateProfile = useCallback(async (updates) => {
    const updatedUser = await authService.updateProfile(updates, session.accessToken);
    setUser(updatedUser);
    return updatedUser;
  }, [session.accessToken]);

  const handleWatchlistCreated = useCallback((watchlist) => {
    setSelectedWatchlistId(watchlist?.id || null);
    setActivePage("view-watchlist");
  }, []);

  const handleOpenWatchlist = useCallback((watchlistId) => {
    setSelectedWatchlistId(watchlistId || null);
    setActivePage("view-watchlist");
  }, []);

  return (
    <div className="flex h-screen w-full">
      <Sidebar
        activePage={activePage}
        onSelectPage={setActivePage}
        onLogout={isLoggedIn ? handleLogout : undefined}
      />

      <main className="flex min-h-0 flex-1 flex-col overflow-hidden bg-[#071316]">
        {activePage === "profile" ? (
          <ProfilePage
            loading={loadingUser}
            user={user}
            onRefresh={handleRefreshProfile}
            onUpdateProfile={handleUpdateProfile}
            onLogout={handleLogout}
            onBackToMap={() => setActivePage("map")}
          />
        ) : activePage === "crime" ? (
          <CrimePage docsUrl={docsUrl} />
        ) : activePage === "collisions" ? (
          <CollisionsPage docsUrl={docsUrl} />
        ) : activePage === "roads" ? (
          <RoadsPage docsUrl={docsUrl} />
        ) : activePage === "watchlist" ? (
          <WatchlistPage
            docsUrl={docsUrl}
            accessToken={session.accessToken}
            onWatchlistCreated={handleWatchlistCreated}
          />
        ) : activePage === "view-watchlist" ? (
          <ViewWatchlistPage
            docsUrl={docsUrl}
            accessToken={session.accessToken}
            selectedWatchlistId={selectedWatchlistId}
            onSelectWatchlist={handleOpenWatchlist}
            onCreateNew={() => setActivePage("watchlist")}
          />
        ) : (
          <MapPage docsUrl={docsUrl} />
        )}
      </main>
    </div>
  );
}

export default App;
