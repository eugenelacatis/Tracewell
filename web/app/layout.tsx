import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";

export const metadata: Metadata = {
  title: "Tracewell",
  description: "LLM observability: traces, spans and eval scores",
};

export default function RootLayout({ children }: LayoutProps<"/">) {
  return (
    <html lang="en">
      <body>
        <header className="topbar">
          <Link href="/traces" className="brand">
            Tracewell
          </Link>
          <nav>
            <Link href="/traces">Traces</Link>
          </nav>
        </header>
        {children}
      </body>
    </html>
  );
}
