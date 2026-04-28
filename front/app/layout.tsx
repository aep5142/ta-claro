import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Ta-Claro",
  description: "Credit-card analytics demo for Ta-Claro",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
