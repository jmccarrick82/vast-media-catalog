import React from "react";
import { BrowserRouter, Routes, Route, Navigate, NavLink } from "react-router-dom";
import HomePage from "./pages/HomePage";
import AssetDossier from "./pages/AssetDossier";
import PersonaPage from "./pages/PersonaPage";
import UseCasePage from "./pages/UseCasePage";
import ArchitecturePage from "./pages/ArchitecturePage";
import SearchPage from "./pages/SearchPage";
import SettingsPage from "./pages/SettingsPage";
import PackagesPage from "./pages/PackagesPage";
import PackageDetailPage from "./pages/PackageDetailPage";

export default function App() {
  return (
    <BrowserRouter>
      <header className="header">
        <div className="container header-inner">
          <NavLink to="/" className="header-logo">
            VAST<span> Content Provenance</span>
          </NavLink>
          <nav className="header-nav">
            <NavLink to="/" end>
              Assets
            </NavLink>
            <NavLink to="/search">
              Search
            </NavLink>
            <NavLink to="/packages">
              Packages
            </NavLink>
            <NavLink to="/architecture">
              Architecture
            </NavLink>
            <NavLink to="/settings">
              Settings
            </NavLink>
          </nav>
        </div>
      </header>
      <main className="container">
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/search" element={<SearchPage />} />
          <Route path="/architecture" element={<ArchitecturePage />} />
          <Route path="/settings" element={<SettingsPage />} />
          <Route path="/packages" element={<PackagesPage />} />
          <Route path="/packages/:packageId" element={<PackageDetailPage />} />
          <Route path="/assets" element={<Navigate to="/" replace />} />
          <Route path="/assets/:assetId" element={<AssetDossier />} />
          {/* Legacy routes kept for backward compat */}
          <Route path="/persona/:personaId" element={<PersonaPage />} />
          <Route path="/usecase/:ucId" element={<UseCasePage />} />
        </Routes>
      </main>
    </BrowserRouter>
  );
}
