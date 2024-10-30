import React, { useState } from 'react';
import { loginUser } from '../utils/api'; // Import API function for login

const LoginPage = () => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();

    // Basic form validation
    if (!username || !password) {
      setError('Username and password are required.');
      return;
    }

    try {
      const response = await loginUser({ username, password });
      if (response.token) {
        // Store JWT securely in local storage (consider HttpOnly cookies for production)
        localStorage.setItem('token', response.token);
        setError('');
        // Redirect to dashboard or home
        window.location.href = '/dashboard';
      } else {
        setError(response.message || 'Login failed.');
      }
    } catch (err) {
      setError('Server error. Please try again later.');
    }
  };

  return (
    <div className="flex flex-col items-center justify-center h-screen">
      <h1 className="text-2xl font-bold mb-4">Login</h1>
      <form className="w-1/3" onSubmit={handleSubmit}>
        <input
          type="text"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          placeholder="Username"
          className="w-full p-2 mb-2 border"
        />
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Password"
          className="w-full p-2 mb-2 border"
        />
        {error && <p className="text-red-500">{error}</p>}
        <button type="submit" className="w-full bg-blue-500 text-white p-2">
          Login
        </button>
      </form>
    </div>
  );
};

export default LoginPage;
