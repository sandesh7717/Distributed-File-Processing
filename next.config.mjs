// Next.js builds the React dashboard; Express serves its static export and API.
// Development only: proxy API calls to the separately running Express server.
export default function config(phase) {
  if (phase === 'phase-development-server')
    return {
      poweredByHeader: false,
      async rewrites() {
        return [
          {
            source: '/api/:path*',
            destination: 'http://127.0.0.1:8080/api/:path*',
          },
        ];
      },
    };
  return { output: 'export', poweredByHeader: false };
}
