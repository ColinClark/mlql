#!/usr/bin/env node

const { spawn } = require('child_process');
const path = require('path');

// Path to the compiled Rust binary
const binaryPath = path.join(__dirname, '../../target/release/mlql-server');

// Spawn the Rust MCP server
const server = spawn(binaryPath, [], {
  stdio: 'inherit',
  env: process.env
});

server.on('error', (err) => {
  console.error('Failed to start MLQL server:', err);
  process.exit(1);
});

server.on('exit', (code) => {
  process.exit(code || 0);
});

// Handle termination signals
process.on('SIGTERM', () => server.kill('SIGTERM'));
process.on('SIGINT', () => server.kill('SIGINT'));
