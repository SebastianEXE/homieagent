import { NextResponse } from 'next/server';

export async function GET() {
  try {
    // Check if worker is running by trying to connect to its status endpoint
    const response = await fetch('http://localhost:3001/status', {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
      // Add a short timeout
      signal: AbortSignal.timeout(2000),
    });
    
    if (response.ok) {
      const data = await response.json();
      return NextResponse.json({ running: true, ...data });
    }
  } catch (error) {
    // Worker is not running or not reachable
    return NextResponse.json({ running: false });
  }
  
  return NextResponse.json({ running: false });
}


