'use client'

import { useState } from 'react'
import useSWR from 'swr'
import clsx from 'clsx'
import StockWidget from './StockWidget'

interface Order {
  orderId: string
  customerId: string
  productId: string
  quantity: number
  totalAmount: number
  status: string
  placedAt: string
}

const STATUS: Record<string, { pill: string; dot: string }> = {
  PENDING:            { pill: 'bg-slate-700/60 text-slate-300 border-slate-600/40',    dot: 'bg-slate-400' },
  PAYMENT_CONFIRMED:  { pill: 'bg-blue-500/15 text-blue-300 border-blue-500/30',       dot: 'bg-blue-400 animate-pulse' },
  STOCK_CONFIRMED:    { pill: 'bg-amber-500/15 text-amber-300 border-amber-500/30',    dot: 'bg-amber-400 animate-pulse' },
  CONFIRMED:          { pill: 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30', dot: 'bg-emerald-400' },
  PAYMENT_FAILED:     { pill: 'bg-red-500/15 text-red-300 border-red-500/30',          dot: 'bg-red-400' },
  STOCK_INSUFFICIENT: { pill: 'bg-amber-600/15 text-amber-400 border-amber-600/30',    dot: 'bg-amber-500' },
  CANCELLED:          { pill: 'bg-slate-800/60 text-slate-500 border-slate-700/40',    dot: 'bg-slate-600' },
}

const fetcher = (url: string) => fetch(url).then(r => r.json())

const inputCls = `w-full bg-[#0a0f1e] border border-[#1e2d4a] rounded-lg px-3 py-2 text-sm text-slate-200
                  placeholder-slate-600 focus:outline-none focus:border-blue-500/60 focus:ring-1 focus:ring-blue-500/20
                  transition-all`

function OrderRow({ orderId, onRemove }: { orderId: string; onRemove: () => void }) {
  const { data } = useSWR<Order>(`/api/orders/${orderId}`, fetcher, { refreshInterval: 2000 })

  const handleCancel = async () => {
    await fetch(`/api/orders/${orderId}`, { method: 'POST' })
  }

  if (!data) return (
    <tr>
      <td colSpan={4} className="px-4 py-3 text-xs text-slate-600">Loading…</td>
    </tr>
  )

  const s = STATUS[data.status] ?? STATUS.PENDING
  return (
    <tr className="border-b border-[#1e2d4a]/50 hover:bg-white/[0.015] transition-colors group">
      <td className="px-4 py-2.5">
        <span className="font-mono text-xs text-slate-500">{data.orderId.slice(0, 8)}</span>
      </td>
      <td className="px-4 py-2.5">
        <span className={clsx('inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[10px] font-semibold border', s.pill)}>
          <span className={clsx('w-1.5 h-1.5 rounded-full shrink-0', s.dot)} />
          {data.status}
        </span>
      </td>
      <td className="px-4 py-2.5 text-xs text-slate-500">
        {data.productId} <span className="text-slate-600">×</span> {data.quantity}
      </td>
      <td className="px-4 py-2.5">
        <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
          {data.status !== 'CANCELLED' && data.status !== 'CONFIRMED' && (
            <button
              onClick={handleCancel}
              className="text-[10px] text-red-400/70 hover:text-red-300 transition-colors"
            >
              Cancel
            </button>
          )}
          <button
            onClick={onRemove}
            className="text-[10px] text-slate-600 hover:text-slate-400 transition-colors"
          >
            ✕
          </button>
        </div>
      </td>
    </tr>
  )
}

export default function OrderPanel() {
  const [orderIds, setOrderIds] = useState<string[]>([])
  const [customerId, setCustomerId] = useState('alice')
  const [productId, setProductId] = useState('prod-1')
  const [quantity, setQuantity] = useState(2)
  const [submitting, setSubmitting] = useState(false)
  const [forcingDlq, setForcingDlq] = useState(false)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setSubmitting(true)
    try {
      const res = await fetch('/api/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ customerId, productId, quantity, totalAmount: quantity * 24.99 }),
      })
      const order = await res.json()
      if (order.orderId) setOrderIds(prev => [order.orderId, ...prev])
    } finally {
      setSubmitting(false)
    }
  }

  const handleForceDlq = async () => {
    setForcingDlq(true)
    try {
      const uuid = crypto.randomUUID()
      await fetch(`/api/payments/force-fail/${uuid}`, { method: 'POST' })
      const res = await fetch('/api/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ orderId: uuid, customerId, productId, quantity, totalAmount: quantity * 24.99 }),
      })
      const order = await res.json()
      if (order.orderId) setOrderIds(prev => [order.orderId, ...prev])
    } finally {
      setForcingDlq(false)
    }
  }

  return (
    <div className="flex flex-col h-full">
      {/* Panel header */}
      <div className="px-4 py-3 shrink-0 border-b border-[#1e2d4a]">
        <h2 className="text-sm font-semibold text-slate-200">Order Panel</h2>
      </div>

      {/* Create Order Form */}
      <form onSubmit={handleSubmit} className="px-4 py-3 shrink-0 border-b border-[#1e2d4a] space-y-2.5">
        <div className="grid grid-cols-[1fr_auto_auto] gap-2">
          <input
            value={customerId}
            onChange={e => setCustomerId(e.target.value)}
            placeholder="Customer ID"
            className={inputCls}
          />
          <select
            value={productId}
            onChange={e => setProductId(e.target.value)}
            className={clsx(inputCls, 'w-auto cursor-pointer')}
          >
            <option value="prod-1">prod-1</option>
            <option value="prod-2">prod-2 (low)</option>
            <option value="prod-3">prod-3</option>
          </select>
          <input
            type="number"
            min={1}
            value={quantity}
            onChange={e => setQuantity(Number(e.target.value))}
            className={clsx(inputCls, 'w-16 text-center')}
          />
        </div>

        <div className="flex gap-2">
          <button
            type="submit"
            disabled={submitting}
            className="flex-1 flex items-center justify-center gap-1.5 bg-blue-600 hover:bg-blue-500
                       disabled:opacity-40 disabled:cursor-not-allowed text-white text-sm font-medium
                       py-2 rounded-lg transition-all active:scale-[0.98]"
          >
            {submitting ? (
              <>
                <svg className="w-3.5 h-3.5 animate-spin" viewBox="0 0 24 24" fill="none">
                  <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="3" className="opacity-25" />
                  <path d="M4 12a8 8 0 018-8" stroke="currentColor" strokeWidth="3" strokeLinecap="round" className="opacity-75" />
                </svg>
                Placing…
              </>
            ) : 'Place Order'}
          </button>
          <button
            type="button"
            onClick={handleForceDlq}
            disabled={forcingDlq}
            title="Force payment failure → DLQ"
            className="flex items-center gap-1.5 px-3 py-2 rounded-lg text-sm font-medium transition-all
                       bg-red-950/60 hover:bg-red-900/60 border border-red-800/50 text-red-400
                       disabled:opacity-40 active:scale-[0.98]"
          >
            {forcingDlq ? '…' : (
              <>
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
                </svg>
                Force DLQ
              </>
            )}
          </button>
        </div>
      </form>

      {/* Orders Table */}
      <div className="flex-1 overflow-y-auto min-h-0">
        {orderIds.length === 0 ? (
          <div className="flex flex-col items-center justify-center gap-2 py-10 text-center px-8">
            <div className="w-10 h-10 rounded-full bg-[#1e2d4a] flex items-center justify-center text-slate-600">
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 10.5V6a3.75 3.75 0 10-7.5 0v4.5m11.356-1.993l1.263 12c.07.665-.45 1.243-1.119 1.243H4.25a1.125 1.125 0 01-1.12-1.243l1.264-12A1.125 1.125 0 015.513 7.5h12.974c.576 0 1.059.435 1.119 1.007z" />
              </svg>
            </div>
            <p className="text-slate-500 text-sm">No orders yet</p>
            <p className="text-slate-600 text-xs">Fill the form above and hit Place Order</p>
          </div>
        ) : (
          <table className="w-full">
            <thead>
              <tr className="border-b border-[#1e2d4a]">
                <th className="px-4 py-2 text-left text-[10px] font-medium text-slate-600 uppercase tracking-wider">Order</th>
                <th className="px-4 py-2 text-left text-[10px] font-medium text-slate-600 uppercase tracking-wider">Status</th>
                <th className="px-4 py-2 text-left text-[10px] font-medium text-slate-600 uppercase tracking-wider">Item</th>
                <th className="px-4 py-2" />
              </tr>
            </thead>
            <tbody>
              {orderIds.map(id => (
                <OrderRow key={id} orderId={id} onRemove={() => setOrderIds(prev => prev.filter(o => o !== id))} />
              ))}
            </tbody>
          </table>
        )}
      </div>

      <StockWidget />
    </div>
  )
}
