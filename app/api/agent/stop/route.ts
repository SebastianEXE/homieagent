import { NextResponse } from 'next/server';

export async function POST() {
  try {
    // Try to stop the worker gracefully
    const response = await fetch('http://localhost:3001/stop', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      signal: AbortSignal.timeout(5000),
    });
    
    if (response.ok) {
      const data = await response.json();
      return NextResponse.json({ success: true, ...data });
    }
    
    return NextResponse.json({ success: false, error: 'Failed to stop worker' }, { status: 500 });
  } catch (error: any) {
    return NextResponse.json({ 
      success: false, 
      error: error.message || 'Worker is not running' 
    }, { status: 500 });
  }
}


