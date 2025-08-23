# SecureBank Enterprise API Server

A banking API server focused here on memory leak investigation and remediation.

## Memory Leak Investigation Guide (only memory-related workflows)

### Step 1: Build and Run the Docker Container

```bash
# Build the Docker image
docker build -t securebank-server .

# Run the container with port mapping
docker run -d -p 8000:8000 --name securebank-container securebank-server

# Verify server is running
curl http://localhost:8000/
```

> Note: restart the server evertything before you run load_test

### Step 2: Run Load Tests to Trigger Memory Leaks

```bash
# Run memory-leak–focused test
python load_test.py memory

# Optionally run multiple memory tests concurrently to accelerate leak observation
python load_test.py memory &
python load_test.py memory &
```

### Step 3: Monitor Memory Growth (Heap/Process Metrics)

**Get real-time memory diagnostics:**
```bash
# Get detailed memory metrics
curl http://localhost:8000/api/v1/system/diagnostics | jq

# Watch memory grow in real-time (compact view)
watch -n 2 'curl -s http://localhost:8000/api/v1/system/diagnostics | jq ".memory_usage_mb, .transaction_cache_size, .active_user_sessions, .pending_reports"'

# Colored dashboard-style view
watch -n 1 --color 'curl -s http://localhost:8000/api/v1/system/diagnostics | jq -C "{
\"🧠 Memory (MB)\": .memory_usage_mb,
\"📦 Cache Size\": .transaction_cache_size,
\"👥 Sessions\": .active_user_sessions,
\"📊 Reports\": .pending_reports,
\"⚠️  Status\": (if .memory_usage_mb > 200 then \"HIGH MEMORY\" elif .transaction_cache_size > 40 then \"CACHE LEAK\" else \"OK\" end)
}"'
```

**Key memory indicators to watch:**
- `memory_usage_mb` - Total memory usage (should not grow unbounded)
- `transaction_cache_size` - Transaction cache (ensure bounded growth/eviction)
- `active_user_sessions` - User sessions (should expire or remain bounded)
- `pending_reports` - Background reports (should not accumulate indefinitely)

### Step 4: Monitor Docker Container Resources (Memory)

```bash
# Watch container resource usage (focus on MEM USAGE)
docker stats securebank-container

# Get container logs (for OOMs or related errors)
docker logs securebank-container -f
```

### What You'll Observe (Memory Leaks)

- Memory usage continuously increases
- Transaction cache grows without bounds
- User sessions accumulate and never expire
- Background reports queue up indefinitely

### Cleanup

```bash
# Stop and remove container
docker stop securebank-container
docker rm securebank-container

# Or restart fresh for another test
docker restart securebank-container

# Clean up system state via API (if applicable)
curl -X POST http://localhost:8000/api/v1/admin/system-maintenance
```

## Technical Implementation Details (Memory-related)

The memory growth in this server can be influenced by:
- Transaction caching for performance (ensure proper eviction)
- Persistent user sessions (ensure expiration/cleanup)
- Background report generation (ensure queue processing/drain)
- Market data processing delays (ensure buffers/queues are bounded)
