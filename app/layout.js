import './globals.css';
export const metadata = {
  title: 'Flow — Distributed file processing',
  description:
    'Upload sales CSVs, monitor distributed processing, and explore regional totals.',
};
export default function Layout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
