import React, { useState } from 'react';
import { registerUser } from '../utils/api'; // Import API function for registration

const RegisterPage = () => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [role, setRole] = useState('user');
  const [error, setError] = useState('');
  const [success, setSuccess] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();

    // Basic form validation
    if (!username || !password) {
      setError('Username and password are required.');
      return;
    }

    try {
      const response = await registerUser({ username, password, role });
      if (response.success) {
        setSuccess(true);
        setError('');
      } else {
        setError(response.message || 'Registration failed.');
      }
    } catch (err) {
      setError('Server error. Please try again later.');
    }
  };

  return (
    <div className="flex flex-col items-center justify-center h-screen">
      <h1 className="text-2xl font-bold mb-4">Register</h1>
      {success ? (
        <p className="text-green-500">Registration successful! Please login.</p>
      ) : (
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
          <select
            value={role}
            onChange={(e) => setRole(e.target.value)}
            className="w-full p-2 mb-2 border"
          >
            <option value="user">User</option>
            <option value="admin">Admin</option>
          </select>
          {error && <p className="text-red-500">{error}</p>}
          <button type="submit" className="w-full bg-blue-500 text-white p-2">
            Register
          </button>
        </form>
      )}
    </div>
  );
};

export default RegisterPage;
