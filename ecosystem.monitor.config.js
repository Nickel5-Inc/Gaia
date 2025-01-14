module.exports = {
  apps: [{
    name: "validator_monitor",
    script: "scripts/validator_monitor.py",
    interpreter: "python3",
    watch: false,
    max_memory_restart: "200M",
    log_date_format: "YYYY-MM-DD HH:mm:ss.SSS",
    error_file: "/var/log/gaia/monitor-error.log",
    out_file: "/var/log/gaia/monitor-out.log",
    merge_logs: true,
    autorestart: true,
    max_restarts: 10,
    restart_delay: 3000
  }]
} 