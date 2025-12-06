import { NextResponse } from 'next/server';

export async function POST() {
  try {
    // Try to start the worker by calling its start endpoint
    const response = await fetch('http://localhost:3001/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      signal: AbortSignal.timeout(5000),
    });
    
    if (response.ok) {
      const data = await response.json();
      return NextResponse.json({ success: true, ...data });
    }
    
    return NextResponse.json({ success: false, error: 'Failed to start worker' }, { status: 500 });
  } catch (error: any) {
    // Worker might not be running - that's okay, we'll return an error
    return NextResponse.json({ 
      success: false, 
      error: error.message || 'Worker is not running. Please start it manually with: npm run start:worker' 
    }, { status: 500 });
  }
}


