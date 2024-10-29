import React from 'react';
import { BrowserRouter as Router, Route, Routes } from 'react-router-dom';
import Navbar from './components/Navbar';
import HomePage from './pages/HomePage';
import LoginPage from './pages/LoginPage';
import RegisterPage from './pages/RegisterPage';
import Dashboard from './pages/Dashboard';
import Governance from './pages/Governance';
import Marketplace from './pages/Marketplace';
import DIDManagement from './pages/DIDManagement';

function App() {
  return (
    <Router>
      <Navbar />
      <div className="container mx-auto p-4">
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/login" element={<LoginPage />} />
          <Route path="/register" element={<RegisterPage />} />
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/governance" element={<Governance />} />
          <Route path="/marketplace" element={<Marketplace />} />
          <Route path="/did-management" element={<DIDManagement />} />
        </Routes>
      </div>
    </Router>
  );
}

export default App;
