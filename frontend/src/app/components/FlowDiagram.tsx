'use client'

import { useCallback, useEffect, useState, memo } from 'react'
import {
  ReactFlow,
  Node,
  Edge,
  Background,
  Controls,
  BackgroundVariant,
  useNodesState,
  useEdgesState,
  Handle,
  Position,
  NodeProps,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import useSWR from 'swr'

interface Notification {
  id: string
  orderId: string
  type: string
  message: string
  sentAt: string
}

// ── Custom node: Service ────────────────────────────────────────────────────
const ServiceNode = memo(({ data }: NodeProps) => {
  const { label, port, color, glowColor } = data as {
    label: string; port: string; color: string; glowColor: string
  }
  return (
    <div
      className="relative px-4 py-2.5 rounded-xl border text-center select-none"
      style={{
        background: `linear-gradient(135deg, ${color}22 0%, ${color}11 100%)`,
        borderColor: `${color}55`,
        boxShadow: `0 0 20px ${glowColor}30, inset 0 1px 0 ${color}20`,
        minWidth: 148,
      }}
    >
      <Handle type="target" position={Position.Left}  style={{ background: color, border: 'none', width: 8, height: 8 }} />
      <Handle type="source" position={Position.Right} style={{ background: color, border: 'none', width: 8, height: 8 }} />
      <Handle type="target" position={Position.Top}   style={{ background: color, border: 'none', width: 8, height: 8 }} />
      <Handle type="source" position={Position.Bottom} style={{ background: color, border: 'none', width: 8, height: 8 }} />
      <div className="text-xs font-bold tracking-wide" style={{ color }}>{label}</div>
      <div className="text-[10px] mt-0.5" style={{ color: `${color}99` }}>:{port}</div>
    </div>
  )
})
ServiceNode.displayName = 'ServiceNode'

// ── Custom node: Topic ──────────────────────────────────────────────────────
const TopicNode = memo(({ data }: NodeProps) => {
  const { label } = data as { label: string }
  return (
    <div
      className="relative px-3 py-1.5 rounded-full text-center select-none"
      style={{
        background: 'linear-gradient(135deg, #1e2d4a 0%, #162038 100%)',
        border: '1px solid #fbbf2455',
        boxShadow: '0 0 12px #fbbf2415',
        minWidth: 130,
      }}
    >
      <Handle type="target" position={Position.Left}  style={{ background: '#fbbf24', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Right} style={{ background: '#fbbf24', border: 'none', width: 6, height: 6 }} />
      <Handle type="target" position={Position.Top}   style={{ background: '#fbbf24', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Bottom} style={{ background: '#fbbf24', border: 'none', width: 6, height: 6 }} />
      <span className="text-[10px] font-mono font-semibold text-amber-400 tracking-wider">{label}</span>
    </div>
  )
})
TopicNode.displayName = 'TopicNode'

const nodeTypes = { service: ServiceNode, topic: TopicNode }

// ── Node definitions ────────────────────────────────────────────────────────
const initialNodes: Node[] = [
  { id: 'order-service',        type: 'service', position: { x: 40,  y: 70  }, data: { label: 'order-service',        port: '8081', color: '#3b82f6', glowColor: '#3b82f6' } },
  { id: 'payment-service',      type: 'service', position: { x: 460, y: 10  }, data: { label: 'payment-service',      port: '8082', color: '#10b981', glowColor: '#10b981' } },
  { id: 'inventory-service',    type: 'service', position: { x: 460, y: 160 }, data: { label: 'inventory-service',    port: '8083', color: '#f59e0b', glowColor: '#f59e0b' } },
  { id: 'notification-service', type: 'service', position: { x: 780, y: 80  }, data: { label: 'notification-service', port: '8084', color: '#8b5cf6', glowColor: '#8b5cf6' } },
  { id: 'topic-orders-placed',      type: 'topic', position: { x: 260, y: 15  }, data: { label: 'orders.placed'      } },
  { id: 'topic-orders-cancelled',   type: 'topic', position: { x: 260, y: 170 }, data: { label: 'orders.cancelled'   } },
  { id: 'topic-payments-processed', type: 'topic', position: { x: 620, y: 10  }, data: { label: 'payments.processed' } },
  { id: 'topic-payments-dlq',       type: 'topic', position: { x: 620, y: 300 }, data: { label: 'payments.dlq'       } },
  { id: 'topic-inventory-reserved', type: 'topic', position: { x: 620, y: 165 }, data: { label: 'inventory.reserved' } },
]

// ── Edge animation map ──────────────────────────────────────────────────────
const ANIMATION_MAP: Record<string, string[]> = {
  PAYMENT_SUCCESS:    ['e-order-placed', 'e-placed-payment',   'e-payment-processed', 'e-processed-order', 'e-processed-notification'],
  PAYMENT_FAILED:     ['e-order-placed', 'e-placed-payment',   'e-payment-dlq'],
  STOCK_RESERVED:     ['e-order-placed', 'e-placed-inventory', 'e-inventory-reserved', 'e-reserved-order', 'e-reserved-notification'],
  STOCK_INSUFFICIENT: ['e-order-placed', 'e-placed-inventory', 'e-inventory-reserved'],
}

const buildEdges = (animatedIds: Set<string>, dlqIds: Set<string>): Edge[] => {
  const edge = (id: string, source: string, target: string, label: string): Edge => {
    const isActive = animatedIds.has(id)
    const isDlq = dlqIds.has(id)
    return {
      id, source, target, label,
      animated: isActive,
      style: {
        stroke: isDlq ? '#ef4444' : isActive ? '#60a5fa' : '#1e3a5f',
        strokeWidth: isActive ? 2.5 : 1.5,
        filter: isActive ? `drop-shadow(0 0 4px ${isDlq ? '#ef4444' : '#60a5fa'})` : 'none',
      },
      labelStyle: { fill: '#475569', fontSize: 9, fontFamily: 'monospace' },
      labelBgStyle: { fill: '#0a0f1e', fillOpacity: 0.9 },
      labelBgPadding: [4, 2] as [number, number],
      labelBgBorderRadius: 3,
    }
  }
  return [
    edge('e-order-placed',           'order-service',              'topic-orders-placed',      'produces'),
    edge('e-order-cancelled',        'order-service',              'topic-orders-cancelled',   'produces'),
    edge('e-placed-payment',         'topic-orders-placed',        'payment-service',          'consumes'),
    edge('e-placed-inventory',       'topic-orders-placed',        'inventory-service',        'consumes'),
    edge('e-payment-processed',      'payment-service',            'topic-payments-processed', 'produces'),
    edge('e-payment-dlq',            'payment-service',            'topic-payments-dlq',       'DLQ'),
    edge('e-inventory-reserved',     'inventory-service',          'topic-inventory-reserved', 'produces'),
    edge('e-processed-order',        'topic-payments-processed',   'order-service',            'consumes'),
    edge('e-processed-notification', 'topic-payments-processed',   'notification-service',     'consumes'),
    edge('e-reserved-order',         'topic-inventory-reserved',   'order-service',            'consumes'),
    edge('e-reserved-notification',  'topic-inventory-reserved',   'notification-service',     'consumes'),
    edge('e-cancelled-inventory',    'topic-orders-cancelled',     'inventory-service',        'consumes'),
  ]
}

const fetcher = (url: string) => fetch(url).then(r => r.json())

export default function FlowDiagram() {
  const [nodes, , onNodesChange] = useNodesState(initialNodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(buildEdges(new Set(), new Set()))
  const [animatedIds, setAnimatedIds] = useState<Set<string>>(new Set())
  const [dlqIds, setDlqIds] = useState<Set<string>>(new Set())
  const [prevCount, setPrevCount] = useState(0)

  const { data: notifications } = useSWR<Notification[]>('/api/notifications', fetcher, {
    refreshInterval: 2000,
  })

  const triggerAnimation = useCallback((edgeIds: string[], isDlq: boolean) => {
    setAnimatedIds(new Set(edgeIds))
    if (isDlq) setDlqIds(new Set(edgeIds))
    setTimeout(() => {
      setAnimatedIds(new Set())
      setDlqIds(new Set())
    }, 3000)
  }, [])

  useEffect(() => {
    if (!notifications || notifications.length <= prevCount) return
    const newNotifs = notifications.slice(prevCount)
    setPrevCount(notifications.length)
    const latest = newNotifs[newNotifs.length - 1]
    const edgeIds = ANIMATION_MAP[latest.type] || []
    if (edgeIds.length) triggerAnimation(edgeIds, latest.type === 'PAYMENT_FAILED')
  }, [notifications, prevCount, triggerAnimation])

  useEffect(() => {
    setEdges(buildEdges(animatedIds, dlqIds))
  }, [animatedIds, dlqIds, setEdges])

  return (
    <div className="w-full h-full" style={{ background: '#0a0f1e' }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        fitView
        fitViewOptions={{ padding: 0.25 }}
        minZoom={0.5}
        maxZoom={2}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={24}
          size={1}
          color="#1e2d4a"
        />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  )
}
