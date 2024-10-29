// components/Navbar.js
import React from 'react';
import { Link } from 'react-router-dom';

const Navbar = () => (
  <nav className="bg-blue-500 p-4">
    <ul className="flex space-x-4 text-white">
      <li><Link to="/">Home</Link></li>
      <li><Link to="/dashboard">Dashboard</Link></li>
      <li><Link to="/governance">Governance</Link></li>
      <li><Link to="/marketplace">Marketplace</Link></li>
      <li><Link to="/did-management">DID Management</Link></li>
    </ul>
  </nav>
);

export default Navbar;
