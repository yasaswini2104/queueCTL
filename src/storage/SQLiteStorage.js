const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

class SQLiteStorage {
  constructor(dbPath = null) {
    const dataDir = path.join(process.cwd(), 'data');
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }

    this.dbPath = dbPath || path.join(dataDir, 'queue.db');
    this.db = new Database(this.dbPath);
    this.initialize();
  }

  initialize() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER DEFAULT 0,
        max_retries INTEGER DEFAULT 3,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        last_error TEXT,
        next_retry_at TEXT,
        completed_at TEXT,
        locked_by TEXT,
        locked_at TEXT
      );

      CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
      CREATE INDEX IF NOT EXISTS idx_jobs_next_retry ON jobs(next_retry_at);
      CREATE INDEX IF NOT EXISTS idx_jobs_locked ON jobs(locked_by);

      CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS workers (
        id TEXT PRIMARY KEY,
        started_at TEXT NOT NULL,
        last_heartbeat TEXT NOT NULL,
        status TEXT NOT NULL
      );
    `);
  }

  saveJob(job) {
    const stmt = this.db.prepare(`
      INSERT OR REPLACE INTO jobs 
      (id, command, state, attempts, max_retries, created_at, updated_at, 
       last_error, next_retry_at, completed_at, locked_by, locked_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    stmt.run(
      job.id,
      job.command,
      job.state,
      job.attempts,
      job.maxRetries,
      job.createdAt,
      job.updatedAt,
      job.lastError,
      job.nextRetryAt,
      job.completedAt,
      null,
      null
    );
  }

  getJob(id) {
    const stmt = this.db.prepare('SELECT * FROM jobs WHERE id = ?');
    return stmt.get(id);
  }

  listJobs(state = null) {
    if (state) {
      const stmt = this.db.prepare('SELECT * FROM jobs WHERE state = ? ORDER BY created_at DESC');
      return stmt.all(state);
    }
    const stmt = this.db.prepare('SELECT * FROM jobs ORDER BY created_at DESC');
    return stmt.all();
  }

  getJobStats() {
    const stmt = this.db.prepare(`
      SELECT state, COUNT(*) as count 
      FROM jobs 
      GROUP BY state
    `);
    return stmt.all();
  }

  acquireJob(workerId) {
    const now = new Date().toISOString();
    
    // Find an available job
    const findStmt = this.db.prepare(`
      SELECT * FROM jobs 
      WHERE state = 'pending' 
        AND (locked_by IS NULL OR locked_at < datetime('now', '-5 minutes'))
      ORDER BY created_at ASC 
      LIMIT 1
    `);
    
    const job = findStmt.get();
    if (!job) return null;

    // Lock the job
    const lockStmt = this.db.prepare(`
      UPDATE jobs 
      SET locked_by = ?, locked_at = ?, state = 'processing', updated_at = ?
      WHERE id = ? AND (locked_by IS NULL OR locked_by = ?)
    `);
    
    const result = lockStmt.run(workerId, now, now, job.id, workerId);
    
    if (result.changes > 0) {
      return this.getJob(job.id);
    }
    
    return null;
  }

  releaseJob(jobId) {
    const stmt = this.db.prepare(`
      UPDATE jobs 
      SET locked_by = NULL, locked_at = NULL
      WHERE id = ?
    `);
    stmt.run(jobId);
  }

  getConfig(key) {
    const stmt = this.db.prepare('SELECT value FROM config WHERE key = ?');
    const result = stmt.get(key);
    return result ? result.value : null;
  }

  setConfig(key, value) {
    const stmt = this.db.prepare(`
      INSERT OR REPLACE INTO config (key, value, updated_at)
      VALUES (?, ?, ?)
    `);
    stmt.run(key, value, new Date().toISOString());
  }

  close() {
    this.db.close();
  }
}

module.exports = SQLiteStorage;