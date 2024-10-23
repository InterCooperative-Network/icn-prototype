# setup.py

from setuptools import setup, find_packages

setup(
    name="icn-prototype",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pycryptodome',         # for cryptographic operations
        'cryptography',         # for additional crypto functionality
        'flask',               # for the API
        'flask-jwt-extended',  # for JWT auth
        'werkzeug',           # for utilities
        'pytest',             # for testing
    ],
    python_requires='>=3.8',
)