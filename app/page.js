'use client';
import { useCallback, useEffect, useRef, useState } from 'react';

function money(value) {
  const n = BigInt(value),
    abs = n < 0n ? -n : n;
  return (
    (n < 0n ? '-' : '') +
    new Intl.NumberFormat('en-US').format(abs / 100n) +
    '.' +
    String(abs % 100n).padStart(2, '0')
  );
}
async function get(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(15000) });
  let data;
  try {
    data = await response.json();
  } catch {
    throw Error('Unexpected server response');
  }
  if (!response.ok) throw Error(data.error || 'Request failed');
  return data;
}
export default function Home() {
  const [jobs, setJobs] = useState(null),
    [connection, setConnection] = useState('Connecting…'),
    [error, setError] = useState('');
  const [file, setFile] = useState(null),
    [message, setMessage] = useState(''),
    [uploading, setUploading] = useState(false),
    [progress, setProgress] = useState(0),
    [drag, setDrag] = useState(false);
  const [selected, setSelected] = useState(null),
    [results, setResults] = useState(null),
    [resultError, setResultError] = useState(''),
    [maxBytes, setMaxBytes] = useState(null);
  const refreshing = useRef(false),
    fileInput = useRef(null),
    selection = useRef(null),
    xhrRef = useRef(null);
  const refresh = useCallback(async () => {
    if (refreshing.current) return;
    refreshing.current = true;
    try {
      setJobs(await get('/api/jobs'));
      setConnection('API connected');
      setError('');
    } catch (e) {
      setConnection('Connection unavailable');
      setError(e.message);
    } finally {
      refreshing.current = false;
    }
  }, []);
  useEffect(() => {
    refresh();
    get('/api/config')
      .then((c) => setMaxBytes(c.max_upload_bytes))
      .catch(() => {});
    const timer = setInterval(() => {
      if (!document.hidden) refresh();
    }, 3000);
    return () => {
      clearInterval(timer);
      xhrRef.current?.abort();
    };
  }, [refresh]);
  async function showResults(job) {
    selection.current = job.job_id;
    setSelected(job);
    setResults(null);
    setResultError('');
    try {
      const data = await get(`/api/jobs/${job.job_id}/results`);
      if (selection.current === job.job_id) setResults(data);
    } catch (e) {
      if (selection.current === job.job_id) setResultError(e.message);
    }
  }
  function submit(event) {
    event.preventDefault();
    if (uploading) return;
    if (!file?.name.toLowerCase().endsWith('.csv')) {
      setMessage('Please choose a CSV file.');
      return;
    }
    if (maxBytes && file.size > maxBytes) {
      setMessage('File exceeds the configured upload limit.');
      return;
    }
    const body = new FormData();
    body.append('file', file);
    const xhr = new XMLHttpRequest();
    xhrRef.current = xhr;
    xhr.open('POST', '/api/jobs');
    setUploading(true);
    setProgress(0);
    setMessage('Uploading and validating your file…');
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable) setProgress((e.loaded / e.total) * 100);
    };
    xhr.onload = () => {
      let data;
      try {
        data = JSON.parse(xhr.responseText);
      } catch {
        data = {
          error: 'Unexpected server response. Check activity before retrying.',
        };
      }
      if (xhr.status === 202) {
        setMessage('File queued. Follow its progress in activity.');
        setFile(null);
        fileInput.current.value = '';
        refresh();
      } else setMessage(data.error || 'Upload failed. Try again.');
    };
    xhr.onerror = () =>
      setMessage('Connection lost. Check activity before retrying the upload.');
    xhr.onabort = () =>
      setMessage('Upload stopped. Check activity before retrying.');
    xhr.onloadend = () => {
      setUploading(false);
      xhrRef.current = null;
    };
    xhr.send(body);
  }
  return (
    <>
      <header>
        <a className="brand" href="/" aria-label="Flow home">
          <span className="mark">≋</span>flow
          <span className="brand-note">FILE PROCESSING</span>
        </a>
        <span className={`connection ${error ? 'error' : jobs ? 'ok' : ''}`}>
          {connection}
        </span>
      </header>
      <main>
        <div className="heading">
          <div>
            <p className="eyebrow">PROCESSING WORKSPACE</p>
            <h1>Your files. In motion.</h1>
            <p className="muted">Turn sales data into regional insights.</p>
          </div>
          <a className="secondary" href="/sample.csv" download>
            Download sample CSV
          </a>
        </div>
        <div className="layout">
          <section className="panel upload-panel">
            <div className="section-title">
              <h2>Upload a file</h2>
            </div>
            <form onSubmit={submit}>
              <label
                className={`dropzone ${drag ? 'drag' : ''}`}
                htmlFor="file"
                onDragOver={(e) => {
                  e.preventDefault();
                  setDrag(true);
                }}
                onDragLeave={() => setDrag(false)}
                onDrop={(e) => {
                  e.preventDefault();
                  setDrag(false);
                  if (!uploading) setFile(e.dataTransfer.files[0] || null);
                }}
              >
                <span className="upload-icon" aria-hidden="true">
                  ↑
                </span>
                <strong>{file ? file.name : 'Drop your CSV here'}</strong>
                <span>or click to browse files</span>
                <input
                  ref={fileInput}
                  id="file"
                  type="file"
                  accept=".csv,text/csv"
                  disabled={uploading}
                  onChange={(e) => setFile(e.target.files[0] || null)}
                />
              </label>
              <div className="file-hint">
                CSV{maxBytes ? ` · Up to ${maxBytes / 1024 ** 3} GiB` : ''}
              </div>
              <button type="submit" disabled={uploading}>
                {uploading ? 'Uploading…' : 'Start processing'}
              </button>
              {uploading && (
                <progress
                  max="100"
                  value={progress}
                  aria-label="Upload progress"
                />
              )}
              <p id="upload-message" role="status">
                {message}
              </p>
            </form>
            <div className="format">
              <h3>A consistent format. Accurate results.</h3>
              <p>Use these four columns, with a header row:</p>
              <code>id,region,amount,timestamp</code>
              <p>
                Amounts accept up to two decimal places. Timestamps use RFC3339.
                Quoted fields and newlines are supported.
              </p>
            </div>
          </section>
          <section className="panel jobs-panel">
            <div className="section-title">
              <h2>Processing activity</h2>
              <button className="text-button" onClick={refresh}>
                Refresh
              </button>
            </div>
            <p className="muted small">
              Latest 100 uploads · Updates every 3 seconds
            </p>
            {error && (
              <p role="alert" className="error">
                {error}. Use Refresh to try again.
              </p>
            )}
            <div id="jobs" aria-live="polite">
              {jobs === null ? (
                <div className="empty">
                  {error ? 'Activity unavailable.' : 'Loading your activity…'}
                </div>
              ) : !jobs.length ? (
                <div className="empty">
                  No files yet. Upload your first CSV to get started.
                </div>
              ) : (
                jobs.map((job) => (
                  <article className="job" key={job.job_id}>
                    <div>
                      <p className="job-name">{job.filename}</p>
                      <span className="job-meta">
                        {job.rows.toLocaleString()} records ·{' '}
                        {new Date(job.created_at).toLocaleString()}
                      </span>
                    </div>
                    <div className="job-action">
                      <span className={`badge ${job.status}`}>
                        {job.status}
                      </span>
                      {job.status === 'completed' ? (
                        <button
                          onClick={() => showResults(job)}
                          aria-label={`View results for ${job.filename}`}
                        >
                          View results
                        </button>
                      ) : job.status === 'failed' ? (
                        <span className="error small">
                          {job.error || 'Processing failed'}
                        </span>
                      ) : (
                        <span className="job-meta">
                          {job.progress}% · {job.completed_chunks}/
                          {job.total_chunks} chunks
                        </span>
                      )}
                    </div>
                    {!['completed', 'failed'].includes(job.status) && (
                      <progress
                        max="100"
                        value={job.progress}
                        aria-label={`${job.filename} processing`}
                      />
                    )}
                  </article>
                ))
              )}
            </div>
          </section>
        </div>
        <section className="panel results-panel">
          <div className="section-title">
            <h2>Regional results</h2>
            <span className="muted small" id="result-file">
              {selected?.filename}
            </span>
          </div>
          {!selected ? (
            <div className="empty">
              <span className="empty-symbol" aria-hidden="true">
                ▥
              </span>
              <h3>A clear view of every region</h3>
              <p>Select a completed file to see its aggregated sales.</p>
            </div>
          ) : resultError ? (
            <div className="empty error" role="alert">
              {resultError}
            </div>
          ) : results === null ? (
            <div className="empty" role="status">
              Loading results…
            </div>
          ) : (
            <>
              <div className="results-summary">
                {[
                  [
                    'Total sales',
                    money(
                      results.reduce(
                        (sum, r) => sum + BigInt(r.total_cents),
                        0n,
                      ),
                    ),
                  ],
                  ['Regions', results.length],
                  ['Records', selected.rows.toLocaleString()],
                ].map(([label, value]) => (
                  <div key={label}>
                    <span>{label}</span>
                    <strong>{value}</strong>
                  </div>
                ))}
              </div>
              <table>
                <thead>
                  <tr>
                    <th scope="col">Region</th>
                    <th scope="col">Total sales</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map((row) => (
                    <tr key={row.region}>
                      <td>{row.region}</td>
                      <td>{money(row.total_cents)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
        </section>
        <footer>
          <span>FLOW / DISTRIBUTED FILE PROCESSING</span>
          <span>Stream. Process. Understand.</span>
        </footer>
      </main>
    </>
  );
}
