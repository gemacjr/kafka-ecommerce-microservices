import { NextRequest, NextResponse } from 'next/server'

const ORDER_SERVICE = process.env.ORDER_SERVICE_URL || 'http://localhost:8081'

export async function GET(_req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params
  const res = await fetch(`${ORDER_SERVICE}/api/orders/${id}`, { cache: 'no-store' })
  if (!res.ok) return NextResponse.json({ error: 'Not found' }, { status: 404 })
  return NextResponse.json(await res.json())
}

export async function POST(_req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params
  const res = await fetch(`${ORDER_SERVICE}/api/orders/${id}/cancel`, { method: 'POST' })
  return NextResponse.json({}, { status: res.status })
}
