import axios from 'axios';

const API_URL = 'http://localhost:5000'; // Adjust to match backend URL

export const registerUser = async (userData) => {
  try {
    const response = await axios.post(`${API_URL}/register`, userData);
    return response.data;
  } catch (error) {
    return { success: false, message: error.response?.data?.message || 'Registration error' };
  }
};

export const loginUser = async (loginData) => {
  try {
    const response = await axios.post(`${API_URL}/login`, loginData);
    return response.data;
  } catch (error) {
    return { token: null, message: error.response?.data?.message || 'Login error' };
  }
};
