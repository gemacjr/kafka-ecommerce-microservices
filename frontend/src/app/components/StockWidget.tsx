'use client'

import useSWR, { mutate } from 'swr'
import clsx from 'clsx'

const PRODUCTS = [
  { id: 'prod-1', label: 'Product 1' },
  { id: 'prod-2', label: 'Product 2' },
  { id: 'prod-3', label: 'Product 3' },
]
const MAX_STOCK = 100
const fetcher = (url: string) => fetch(url).then(r => r.json())

function StockBar({ id, label }: { id: string; label: string }) {
  const { data } = useSWR(`/api/inventory/${id}`, fetcher, { refreshInterval: 5000 })
  const stock: number = data?.stock ?? 0
  const pct = Math.min((stock / MAX_STOCK) * 100, 100)

  const isLow      = stock <= 5
  const isMedium   = !isLow && stock <= 20
  const barColor   = isLow ? 'bg-red-500' : isMedium ? 'bg-amber-400' : 'bg-emerald-400'
  const textColor  = isLow ? 'text-red-400' : isMedium ? 'text-amber-400' : 'text-emerald-400'
  const glowColor  = isLow ? 'shadow-red-500/30' : isMedium ? 'shadow-amber-400/30' : 'shadow-emerald-400/30'

  const handleRestock = async () => {
    await fetch(`/api/inventory/${id}/restock?quantity=50`, { method: 'POST' })
    mutate(`/api/inventory/${id}`)
  }

  return (
    <div className="flex items-center gap-3">
      <div className="w-20 shrink-0">
        <p className="text-xs font-medium text-slate-400">{label}</p>
        <p className="text-[10px] text-slate-600 font-mono">{id}</p>
      </div>

      <div className="flex-1 relative h-1.5 bg-[#1e2d4a] rounded-full overflow-hidden">
        <div
          className={clsx('h-full rounded-full transition-all duration-700 shadow-sm', barColor, glowColor)}
          style={{ width: `${pct}%` }}
        />
      </div>

      <span className={clsx('text-xs font-mono font-semibold w-8 text-right tabular-nums', textColor)}>
        {stock}
      </span>

      <button
        onClick={handleRestock}
        className="text-[10px] font-semibold text-slate-500 hover:text-slate-200
                   bg-[#1e2d4a] hover:bg-[#2d4070] px-2 py-0.5 rounded transition-all"
        title="Restock +50"
      >
        +50
      </button>
    </div>
  )
}

export default function StockWidget() {
  return (
    <div className="shrink-0 border-t border-[#1e2d4a] px-4 py-3">
      <div className="flex items-center justify-between mb-3">
        <p className="text-[11px] font-semibold text-slate-500 uppercase tracking-wider">Stock Levels</p>
        <span className="text-[10px] text-slate-600">polls every 5s</span>
      </div>
      <div className="space-y-2.5">
        {PRODUCTS.map(p => <StockBar key={p.id} id={p.id} label={p.label} />)}
      </div>
    </div>
  )
}
