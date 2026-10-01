import { NextRequest, NextResponse } from 'next/server'

const PAYMENT_SERVICE = process.env.PAYMENT_SERVICE_URL || 'http://localhost:8082'

export async function POST(_req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params
  const res = await fetch(`${PAYMENT_SERVICE}/api/payments/force-fail/${id}`, { method: 'POST' })
  return NextResponse.json(await res.json())
}
