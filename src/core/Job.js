const { v4: uuidv4 } = require('uuid');

class Job {
  constructor(data) {
    this.id = data.id || uuidv4();
    this.command = data.command;
    this.state = data.state || 'pending';
    this.attempts = data.attempts || 0;
    this.maxRetries = data.max_retries || data.maxRetries || 3;
    this.createdAt = data.created_at || data.createdAt || new Date().toISOString();
    this.updatedAt = data.updated_at || data.updatedAt || new Date().toISOString();
    this.lastError = data.last_error || data.lastError || null;
    this.nextRetryAt = data.next_retry_at || data.nextRetryAt || null;
    this.completedAt = data.completed_at || data.completedAt || null;
  }

  toJSON() {
    return {
      id: this.id,
      command: this.command,
      state: this.state,
      attempts: this.attempts,
      max_retries: this.maxRetries,
      created_at: this.createdAt,
      updated_at: this.updatedAt,
      last_error: this.lastError,
      next_retry_at: this.nextRetryAt,
      completed_at: this.completedAt,
    };
  }

  markProcessing() {
    this.state = 'processing';
    this.updatedAt = new Date().toISOString();
  }

  markCompleted() {
    this.state = 'completed';
    this.completedAt = new Date().toISOString();
    this.updatedAt = this.completedAt;
  }

  markFailed(error, nextRetryDelay) {
    this.attempts += 1;
    this.lastError = error;
    this.updatedAt = new Date().toISOString();

    if (this.attempts >= this.maxRetries) {
      this.state = 'dead';
    } else {
      this.state = 'failed';
      if (nextRetryDelay) {
        const nextRetry = new Date(Date.now() + nextRetryDelay * 1000);
        this.nextRetryAt = nextRetry.toISOString();
      }
    }
  }

  canRetry() {
    if (this.state !== 'failed') return false;
    if (!this.nextRetryAt) return true;
    return new Date(this.nextRetryAt) <= new Date();
  }

  reset() {
    this.state = 'pending';
    this.attempts = 0;
    this.lastError = null;
    this.nextRetryAt = null;
    this.updatedAt = new Date().toISOString();
  }
}

module.exports = Job;