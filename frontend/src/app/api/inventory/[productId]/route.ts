import { NextRequest, NextResponse } from 'next/server'

const INVENTORY_SERVICE = process.env.INVENTORY_SERVICE_URL || 'http://localhost:8083'

export async function GET(_req: NextRequest, { params }: { params: Promise<{ productId: string }> }) {
  const { productId } = await params
  const res = await fetch(`${INVENTORY_SERVICE}/api/inventory/${productId}`, { cache: 'no-store' })
  return NextResponse.json(await res.json())
}
