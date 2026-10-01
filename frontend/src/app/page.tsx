import dynamic from 'next/dynamic'
import EventLog from './components/EventLog'
import OrderPanel from './components/OrderPanel'

const FlowDiagram = dynamic(() => import('./components/FlowDiagram'), { ssr: false })

export default function DashboardPage() {
  return (
    <div className="flex flex-col h-full overflow-hidden">

      {/* Header */}
      <header className="shrink-0 flex items-center justify-between px-6 py-3
                         bg-[#0f1729]/80 backdrop-blur border-b border-[#1e2d4a]">
        <div className="flex items-center gap-3">
          {/* Animated live indicator */}
          <span className="relative flex h-2.5 w-2.5">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-60" />
            <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-emerald-400" />
          </span>
          <h1 className="text-sm font-semibold tracking-wide text-slate-100">
            Kafka Flow Dashboard
          </h1>
          <span className="hidden sm:inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full
                           bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-[10px] font-medium">
            LIVE
          </span>
        </div>

        <nav className="flex items-center gap-1">
          {[
            { label: 'Kafka UI',   href: 'http://localhost:8080' },
            { label: 'Orders API', href: 'http://localhost:8081/swagger-ui.html' },
            { label: 'Payments',   href: 'http://localhost:8082/swagger-ui.html' },
            { label: 'Inventory',  href: 'http://localhost:8083/swagger-ui.html' },
            { label: 'Notify',     href: 'http://localhost:8084/swagger-ui.html' },
          ].map(({ label, href }) => (
            <a
              key={label}
              href={href}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1 px-2.5 py-1.5 rounded-lg text-xs text-slate-400
                         hover:text-slate-200 hover:bg-white/5 transition-all"
            >
              {label}
              <svg className="w-2.5 h-2.5 opacity-50" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
              </svg>
            </a>
          ))}
        </nav>
      </header>

      {/* Flow Diagram */}
      <div className="shrink-0 h-[52vh] border-b border-[#1e2d4a] relative">
        {/* subtle grid overlay */}
        <div className="absolute inset-0 pointer-events-none"
             style={{ backgroundImage: 'radial-gradient(circle at 50% 50%, rgba(59,130,246,0.04) 0%, transparent 70%)' }} />
        <FlowDiagram />
      </div>

      {/* Bottom panels */}
      <div className="flex flex-1 overflow-hidden min-h-0">
        <div className="w-1/2 border-r border-[#1e2d4a] overflow-y-auto">
          <OrderPanel />
        </div>
        <div className="w-1/2 overflow-y-auto">
          <EventLog />
        </div>
      </div>
    </div>
  )
}
