'use client'

import useSWR from 'swr'
import { useEffect, useRef, useState } from 'react'
import clsx from 'clsx'

interface Notification {
  id: string
  orderId: string
  type: string
  message: string
  sentAt: string
}

const TYPE_CONFIG: Record<string, { bar: string; badge: string; text: string; label: string }> = {
  PAYMENT_SUCCESS:    { bar: 'bg-emerald-500', badge: 'bg-emerald-500/15 border-emerald-500/30', text: 'text-emerald-400', label: 'PAYMENT_SUCCESS'    },
  PAYMENT_FAILED:     { bar: 'bg-red-500',     badge: 'bg-red-500/15 border-red-500/30',         text: 'text-red-400',     label: 'PAYMENT_FAILED'     },
  STOCK_RESERVED:     { bar: 'bg-blue-500',    badge: 'bg-blue-500/15 border-blue-500/30',        text: 'text-blue-400',    label: 'STOCK_RESERVED'     },
  STOCK_INSUFFICIENT: { bar: 'bg-amber-500',   badge: 'bg-amber-500/15 border-amber-500/30',      text: 'text-amber-400',   label: 'STOCK_INSUFFICIENT' },
}

const fetcher = (url: string) => fetch(url).then(r => r.json())

function timeAgo(iso: string) {
  const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (diff < 60)   return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  return `${Math.floor(diff / 3600)}h ago`
}

export default function EventLog() {
  const { data } = useSWR<Notification[]>('/api/notifications', fetcher, { refreshInterval: 2000 })
  const [newIds, setNewIds] = useState<Set<string>>(new Set())
  const prevIds = useRef<Set<string>>(new Set())

  useEffect(() => {
    if (!data) return
    const current = new Set(data.map(n => n.id))
    const fresh = new Set([...current].filter(id => !prevIds.current.has(id)))
    if (fresh.size) {
      setNewIds(fresh)
      setTimeout(() => setNewIds(new Set()), 1500)
    }
    prevIds.current = current
  }, [data])

  const sorted = data ? [...data].reverse().slice(0, 50) : []

  return (
    <div className="flex flex-col h-full">
      {/* Panel header */}
      <div className="px-4 py-3 shrink-0 border-b border-[#1e2d4a] flex items-center justify-between">
        <div>
          <h2 className="text-sm font-semibold text-slate-200">Event Log</h2>
          <p className="text-[11px] text-slate-500 mt-0.5">Live · polling every 2s</p>
        </div>
        {sorted.length > 0 && (
          <span className="text-[10px] font-mono text-slate-600 bg-[#1e2d4a] px-2 py-0.5 rounded-full">
            {sorted.length}
          </span>
        )}
      </div>

      {sorted.length === 0 ? (
        <div className="flex-1 flex flex-col items-center justify-center gap-3 text-center px-8">
          <div className="w-10 h-10 rounded-full bg-[#1e2d4a] flex items-center justify-center text-slate-600">
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M9.348 14.652a3.75 3.75 0 010-5.304m5.304 0a3.75 3.75 0 010 5.304m-7.425 2.121a6.75 6.75 0 010-9.546m9.546 0a6.75 6.75 0 010 9.546" />
            </svg>
          </div>
          <p className="text-slate-500 text-sm">Waiting for events…</p>
          <p className="text-slate-600 text-xs">Place an order to get started</p>
        </div>
      ) : (
        <div className="flex-1 overflow-y-auto">
          {sorted.map(n => {
            const cfg = TYPE_CONFIG[n.type] ?? { bar: 'bg-slate-500', badge: 'bg-slate-500/15 border-slate-500/30', text: 'text-slate-400', label: n.type }
            return (
              <div
                key={n.id}
                className={clsx(
                  'group flex items-start gap-0 border-b border-[#1e2d4a]/60 transition-colors duration-300',
                  newIds.has(n.id) ? 'bg-amber-400/5' : 'hover:bg-white/[0.02]'
                )}
              >
                {/* Left accent bar */}
                <div className={clsx('w-0.5 self-stretch shrink-0 rounded-r', cfg.bar)} />

                <div className="flex-1 flex items-start gap-3 px-3 py-2.5">
                  {/* Badge */}
                  <span className={clsx(
                    'shrink-0 mt-0.5 text-[9px] font-bold font-mono tracking-wider px-1.5 py-0.5 rounded border',
                    cfg.badge, cfg.text
                  )}>
                    {cfg.label}
                  </span>

                  {/* Content */}
                  <div className="flex-1 min-w-0">
                    <p className="text-xs text-slate-400 truncate">{n.message}</p>
                    <p className="text-[10px] text-slate-600 font-mono mt-0.5">{n.orderId.slice(0, 8)}…</p>
                  </div>

                  {/* Time */}
                  <span className="text-[10px] text-slate-600 shrink-0 tabular-nums">{timeAgo(n.sentAt)}</span>
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
