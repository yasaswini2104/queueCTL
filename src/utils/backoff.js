function calculateBackoff(attempt, base = 2) {
  return Math.pow(base, attempt);
}

module.exports = { calculateBackoff };