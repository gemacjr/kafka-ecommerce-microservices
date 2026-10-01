import { NextRequest, NextResponse } from 'next/server'

const INVENTORY_SERVICE = process.env.INVENTORY_SERVICE_URL || 'http://localhost:8083'

export async function POST(req: NextRequest, { params }: { params: Promise<{ productId: string }> }) {
  const { productId } = await params
  const { searchParams } = new URL(req.url)
  const quantity = searchParams.get('quantity') || '50'
  const res = await fetch(`${INVENTORY_SERVICE}/api/inventory/${productId}/restock?quantity=${quantity}`, {
    method: 'POST',
  })
  return NextResponse.json(await res.json())
}
