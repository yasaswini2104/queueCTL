const { exec } = require('child_process');
const { promisify } = require('util');

const execPromise = promisify(exec);

class Executor {
  async execute(command, timeout = 30000) {
    try {
      const { stdout, stderr } = await execPromise(command, {
        timeout,
        shell: '/bin/bash',
      });
      return { success: true, stdout, stderr, exitCode: 0 };
    } catch (error) {
      return {
        success: false,
        stdout: error.stdout || '',
        stderr: error.stderr || error.message,
        exitCode: error.code || 1,
        error: error.message,
      };
    }
  }
}

module.exports = Executor;