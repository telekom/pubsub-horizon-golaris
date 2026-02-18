# Full E2E Test: 2 Golaris Instances Against Real Infrastructure

## Context

We implemented parallelization support for golaris (branch `feat/parallelization-support`) with 4 fixes: return-vs-continue bugs, distributed cancellation via ContainsKey, SSE-to-callback listener guard, and lease-based locks. We want to validate these changes by running 2 golaris instances simultaneously against real Kafka, MongoDB, and Hazelcast — confirming distributed locking and mutual exclusion work in practice.

## Infrastructure Setup

### Step 1: Scale up K8s services in `pandora` namespace

The `pandora` namespace on `laptop-dev-system` has Kafka, Zookeeper, and MongoDB statefulsets (currently scaled to 0) with existing PVCs containing data.

```bash
# Scale up Zookeeper first (Kafka depends on it)
powershell.exe kubectl --context=laptop-dev-system -n pandora scale statefulset horizon-kafka-zookeeper --replicas=1

# Wait for Zookeeper to be ready, then scale Kafka
powershell.exe kubectl --context=laptop-dev-system -n pandora scale statefulset horizon-kafka --replicas=1

# Scale MongoDB (2 replicas for replica set)
powershell.exe kubectl --context=laptop-dev-system -n pandora scale statefulset horizon-mongodb --replicas=2
```

Wait for all pods to be Running/Ready.

### Step 2: Run Hazelcast locally in Docker

No Hazelcast statefulset exists in K8s. Run one locally:

```bash
docker run -d --name hz-golaris-test \
  -p 5701:5701 \
  -e HZ_CLUSTERNAME=dev \
  hazelcast/hazelcast:5.3
```

### Step 3: Port-forward Kafka and MongoDB

In separate terminals (or background PowerShell processes):

```bash
# Kafka (9092)
powershell.exe kubectl --context=laptop-dev-system -n pandora port-forward svc/horizon-kafka 9092:9092 &

# MongoDB (27017)
powershell.exe kubectl --context=laptop-dev-system -n pandora port-forward svc/horizon-mongodb-headless 27017:27017 &
```

**Note:** Kafka advertises as `horizon-kafka-0.horizon-kafka-headless.pandora.svc.cluster.local:9092`. Port-forwarding alone won't work because the client will try to connect to the advertised address. Two options:
- **Option A (recommended):** Add `/etc/hosts` entry: `127.0.0.1 horizon-kafka-0.horizon-kafka-headless.pandora.svc.cluster.local`
- **Option B:** Temporarily patch the Kafka statefulset to advertise `localhost:9092`

MongoDB credentials: `root` / `ineedcoffee` (base64 decoded from K8s secret).

MongoDB connection string: `mongodb://root:ineedcoffee@localhost:27017/?authSource=admin&directConnection=true`

### Step 4: Build golaris locally

```bash
cd /home/hannah/projects/mono-folder/horizon-components/pubsub-horizon-golaris
CGO_ENABLED=0 /usr/local/go/bin/go build -o golaris .
```

### Step 5: Create config files for 2 instances

**Instance 1** (`config-1.yml`):
```yaml
logLevel: debug
port: 8081
hazelcast:
  serviceDNS: "localhost:5701"
  clusterName: "dev"
kafka:
  brokers:
    - "localhost:9092"
mongo:
  url: "mongodb://root:ineedcoffee@localhost:27017/?authSource=admin&directConnection=true"
  database: "horizon"
  collection: "status"
security:
  enabled: false
tracing:
  enabled: false
```

**Instance 2** (`config-2.yml`): Same as above but `port: 8082`.

### Step 6: Run 2 instances

```bash
# Terminal 1
cp config-1.yml config.yml && ./golaris serve

# Terminal 2
cp config-2.yml config.yml && ./golaris serve
```

## Verification

1. **Startup:** Both instances connect to Hazelcast, Kafka, and MongoDB without errors
2. **Distributed locking:** Watch logs — when both instances start their scheduled checks (circuit breaker, republishing, handler checks), only ONE should acquire each lock at a time
3. **Lease auto-release:** Kill instance 1 (Ctrl+C or kill -9). Instance 2 should acquire the previously held locks after 60s lease expiry
4. **Mutual exclusion:** With both running, create test data in MongoDB/Hazelcast and observe that only one instance processes each subscription's republishing

## Teardown

```bash
# Stop local processes
# Remove Hazelcast container
docker rm -f hz-golaris-test

# Scale down K8s
powershell.exe kubectl --context=laptop-dev-system -n pandora scale statefulset horizon-kafka --replicas=0
powershell.exe kubectl --context=laptop-dev-system -n pandora scale statefulset horizon-kafka-zookeeper --replicas=0
powershell.exe kubectl --context=laptop-dev-system -n pandora scale statefulset horizon-mongodb --replicas=0

# Clean up config/binary
rm -f config.yml config-1.yml config-2.yml golaris
```
