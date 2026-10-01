import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Kafka Flow Dashboard',
  description: 'Real-time Kafka microservices event flow visualization',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="h-full">
      <body className="h-full overflow-hidden bg-[#0a0f1e] text-slate-100 antialiased">{children}</body>
    </html>
  )
}
