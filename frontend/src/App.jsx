import { useMemo, useState } from "react";
import CrimePage from "./components/CrimePage";
import MapPage from "./components/MapPage";
import Sidebar from "./components/Sidebar";
import { config } from "./config/env";

function App() {
  const [activePage, setActivePage] = useState("map");
  const docsUrl = useMemo(() => `${config.apiBaseUrl}/docs`, []);

  return (
    <div className="flex h-screen w-full">
      <Sidebar activePage={activePage} onSelectPage={setActivePage} />

      <main className="flex min-h-0 flex-1 flex-col overflow-hidden bg-[#071316]">
        {activePage === "crime" ? <CrimePage docsUrl={docsUrl} /> : <MapPage docsUrl={docsUrl} />}
      </main>
    </div>
  );
}

export default App;
