import { NextResponse } from 'next/server'

const NOTIFICATION_SERVICE = process.env.NOTIFICATION_SERVICE_URL || 'http://localhost:8084'

export async function GET() {
  const res = await fetch(`${NOTIFICATION_SERVICE}/api/notifications`, { cache: 'no-store' })
  return NextResponse.json(await res.json())
}
