# Design: Next.js Frontend — Live Kafka Flow Dashboard

**Date:** 2026-04-04
**Project:** kafka-springboot-project
**Scope:** Add a Next.js 15 App Router frontend that visualizes real-time Kafka event flow across the 4 Spring Boot microservices.

---

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Visualization | Both animated architecture diagram + live event log | Makes the system intuitive and dynamic |
| Real-time data | Polling via Next.js API routes (2s interval) | No backend changes needed; avoids CORS; fast enough to feel real-time |
| Diagram library | React Flow | Purpose-built for node-edge diagrams; animated edges with minimal code |
| Styling | Tailwind CSS | Rapid layout; dark theme suits a technical dashboard |
| Data fetching | SWR | Built-in polling, deduplication, revalidation |

---

## Section 1: Project Structure

New `frontend/` directory at the project root — not a Maven module, co-located alongside Java services.

```
kafka-springboot-project/
└── frontend/
    ├── Dockerfile
    ├── package.json
    ├── next.config.ts
    ├── tailwind.config.ts
    ├── tsconfig.json
    └── src/
        └── app/
            ├── layout.tsx
            ├── page.tsx                        # Main dashboard
            ├── globals.css
            ├── api/                            # Proxy routes (server-side)
            │   ├── orders/
            │   │   └── route.ts               # POST (create order)
            │   ├── orders/[id]/
            │   │   └── route.ts               # GET (status), POST /cancel
            │   ├── notifications/
            │   │   └── route.ts               # GET all notifications
            │   ├── inventory/[productId]/
            │   │   └── route.ts               # GET stock level
            │   └── payments/force-fail/[id]/
            │       └── route.ts               # POST force-fail
            └── components/
                ├── FlowDiagram.tsx            # React Flow architecture diagram
                ├── EventLog.tsx               # Live scrolling notification feed
                ├── OrderPanel.tsx             # Order form + order list + stock widget
                └── StockWidget.tsx            # Product stock level bars
```

### Docker Compose Addition
```yaml
frontend:
  build: ./frontend
  container_name: frontend
  ports:
    - "3000:3000"
  depends_on:
    - order-service
    - payment-service
    - inventory-service
    - notification-service
  environment:
    ORDER_SERVICE_URL: http://order-service:8081
    PAYMENT_SERVICE_URL: http://payment-service:8082
    INVENTORY_SERVICE_URL: http://inventory-service:8083
    NOTIFICATION_SERVICE_URL: http://notification-service:8084
  networks:
    - kafka-net
```

### Dockerfile
```dockerfile
FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:20-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public
EXPOSE 3000
CMD ["node", "server.js"]
```

Requires `output: 'standalone'` in `next.config.ts`.

---

## Section 2: API Proxy Layer

All Next.js API routes run server-side. They read service URLs from environment variables so the same Docker image works both in Docker Compose (container names) and locally (localhost).

| Route | Method | Proxies to | Purpose |
|---|---|---|---|
| `/api/orders` | POST | `order-service:8081/api/orders` | Create new order |
| `/api/orders/[id]` | GET | `order-service:8081/api/orders/{id}` | Poll order status |
| `/api/orders/[id]` | POST (body: `{action:"cancel"}`) | `order-service:8081/api/orders/{id}/cancel` | Cancel order |
| `/api/notifications` | GET | `notification-service:8084/api/notifications` | Fetch all notifications |
| `/api/inventory/[productId]` | GET | `inventory-service:8083/api/inventory/{productId}` | Get stock level |
| `/api/payments/force-fail/[id]` | POST | `payment-service:8082/api/payments/force-fail/{id}` | Register forced failure |
| `/api/inventory/[productId]/restock` | POST | `inventory-service:8083/api/inventory/{productId}/restock?quantity=N` | Restock product |

### Animation Trigger Logic
On every 2-second SWR revalidation of `/api/notifications`, the frontend diffs the previous vs current notification list. For each new notification:

```
PAYMENT_SUCCESS / PAYMENT_FAILED →
  animate edges: order-service→orders.placed, orders.placed→payment-service,
                 payment-service→payments.processed, payments.processed→order-service,
                 payments.processed→notification-service

STOCK_RESERVED / STOCK_INSUFFICIENT →
  animate edges: order-service→orders.placed, orders.placed→inventory-service,
                 inventory-service→inventory.reserved, inventory.reserved→order-service,
                 inventory.reserved→notification-service
```

DLQ events (detected by notification type) animate the `payment-service→payments.dlq` edge in red.
Animated edges stay active for **3 seconds**, then return to static.

---

## Section 3: Flow Diagram (React Flow)

### Nodes

**Service nodes** (rounded rectangle, 160×60px):

| Node ID | Label | Color |
|---|---|---|
| `order-service` | order-service :8081 | Blue (#3B82F6) |
| `payment-service` | payment-service :8082 | Green (#10B981) |
| `inventory-service` | inventory-service :8083 | Orange (#F59E0B) |
| `notification-service` | notification-service :8084 | Purple (#8B5CF6) |

**Topic nodes** (pill shape, 140×36px, dark gray #1F2937, yellow border):

| Node ID | Label |
|---|---|
| `topic-orders-placed` | orders.placed |
| `topic-orders-cancelled` | orders.cancelled |
| `topic-payments-processed` | payments.processed |
| `topic-payments-dlq` | payments.dlq |
| `topic-inventory-reserved` | inventory.reserved |

### Edges (directed, labeled)

| From | To | Label | Normal color | Animated color |
|---|---|---|---|---|
| order-service | topic-orders-placed | produces | gray | blue |
| order-service | topic-orders-cancelled | produces | gray | blue |
| topic-orders-placed | payment-service | consumes | gray | green |
| topic-orders-placed | inventory-service | consumes | gray | orange |
| topic-payments-processed | order-service | consumes | gray | blue |
| topic-payments-processed | notification-service | consumes | gray | purple |
| topic-inventory-reserved | order-service | consumes | gray | blue |
| topic-inventory-reserved | notification-service | consumes | gray | purple |
| payment-service | topic-payments-processed | produces | gray | green |
| payment-service | topic-payments-dlq | produces (DLQ) | gray | red |
| inventory-service | topic-inventory-reserved | produces | gray | orange |

### Interactivity
- Nodes are draggable (React Flow default behaviour)
- Clicking a service node shows a tooltip with: port, consumer group ID, last event received
- `fitView` on initial load; zoom/pan enabled
- Dark background (`#0F172A`) to make colored nodes pop

---

## Section 4: UI Components

### Page Layout
```
┌─────────────────────────────────────────────────────────┐
│  Kafka Flow Dashboard          [Kafka UI ↗] [Docs ↗]    │  ← Header
├─────────────────────────────────────────────────────────┤
│                                                         │
│              FlowDiagram (React Flow)                   │  ← 55vh
│                                                         │
├──────────────────────────┬──────────────────────────────┤
│      OrderPanel          │         EventLog              │  ← flex-1
│                          │                               │
└──────────────────────────┴───────────────────────────────┘
```

### OrderPanel
- **Create Order form:** `customerId` (text input), `productId` (select: prod-1/prod-2/prod-3), `quantity` (number, min 1), Submit button
- On submit → `POST /api/orders` → store returned `orderId` in `useState` array
- **Order table:** columns: orderId (8 chars + copy icon), status (colored badge), age (relative time), actions (Cancel | Force DLQ)
  - Status badge colors: PENDING=gray, PAYMENT_CONFIRMED=blue, STOCK_CONFIRMED=orange, CONFIRMED=green, PAYMENT_FAILED=red, STOCK_INSUFFICIENT=amber, CANCELLED=gray strikethrough
- **Force DLQ flow:** "Force DLQ" button → frontend generates a UUID → `POST /api/payments/force-fail/{uuid}` → `POST /api/orders` with `orderId: uuid` in the body → order-service uses the provided UUID → payment-service throws `PaymentProcessingException` → DLQ edge animates red. **Requires minor backend change:** `CreateOrderRequest` adds an optional `orderId: String` field; `OrderService.createOrder()` uses it if non-null, otherwise generates a UUID.
- **StockWidget** below the table: three horizontal bars (prod-1, prod-2, prod-3), color: green if >20, amber if ≤20, red if ≤5. Each bar has a "+50" Restock button → `POST /api/inventory/{productId}/restock?quantity=50`. Polls every 5s.

### EventLog
- Polls `GET /api/notifications` every 2s via SWR
- New entries slide in at top with a 500ms yellow highlight flash
- Max 50 entries, oldest drop off
- Each row:
  - Colored dot: green (SUCCESS/RESERVED), red (FAILED/INSUFFICIENT), orange (DLQ)
  - Type label (bold, monospace)
  - orderId (truncated to 8 chars, click to copy full)
  - Relative timestamp (e.g. "2s ago")
- Empty state: "Waiting for events... Place an order to get started."

---

## Section 5: Key Dependencies

```json
{
  "dependencies": {
    "next": "^15.0.0",
    "react": "^19.0.0",
    "react-dom": "^19.0.0",
    "@xyflow/react": "^12.0.0",
    "swr": "^2.2.0",
    "tailwindcss": "^4.0.0",
    "clsx": "^2.0.0"
  }
}
```

Note: `@xyflow/react` is the React Flow v12 package name (formerly `reactflow`).

---

## Files to Create

```
frontend/Dockerfile
frontend/package.json
frontend/next.config.ts
frontend/tailwind.config.ts
frontend/tsconfig.json
frontend/src/app/layout.tsx
frontend/src/app/page.tsx
frontend/src/app/globals.css
frontend/src/app/api/orders/route.ts
frontend/src/app/api/orders/[id]/route.ts
frontend/src/app/api/notifications/route.ts
frontend/src/app/api/inventory/[productId]/route.ts
frontend/src/app/api/payments/force-fail/[id]/route.ts
frontend/src/app/api/inventory/[productId]/restock/route.ts
frontend/src/app/components/FlowDiagram.tsx
frontend/src/app/components/EventLog.tsx
frontend/src/app/components/OrderPanel.tsx
frontend/src/app/components/StockWidget.tsx
```

### Modified Files
```
docker-compose.yml                                                                          # add frontend service
order-service/src/main/java/com/kafkademo/order/model/CreateOrderRequest.java              # add optional orderId field
order-service/src/main/java/com/kafkademo/order/service/OrderService.java                  # use provided orderId if non-null
```

---

## Verification

1. `docker-compose up --build` — all 5 containers start (Zookeeper, Kafka, Kafka UI, 4 Spring Boot, frontend)
2. Open `http://localhost:3000` — dashboard loads with React Flow diagram visible
3. Place an order via the form → orderId appears in order table with PENDING status
4. Within 4–6 seconds → status advances to CONFIRMED, edges animate, event log shows PAYMENT_SUCCESS + STOCK_RESERVED
5. Click "Force DLQ" → red edge animation on `payment-service → payments.dlq`
6. Change prod-2 quantity to 10 → STOCK_INSUFFICIENT, amber edge animation
7. Click "Restock" on prod-2 from stock widget → stock bar refills
