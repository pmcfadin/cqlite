# CQLite Installation and Setup Guide

## 🎯 **Complete Installation Solution**

This comprehensive guide provides detailed instructions for installing and configuring CQLite across all supported platforms and deployment scenarios.

---

## 📋 **Installation Overview**

### **Supported Platforms**

| Platform | Architecture | Status | Installation Method |
|----------|-------------|--------|-------------------|
| **Linux** | x86_64 | ✅ Full Support | Binary, Package Manager, Source |
| **Linux** | ARM64 | ✅ Full Support | Binary, Package Manager, Source |
| **macOS** | x86_64 | ✅ Full Support | Homebrew, Binary, Source |
| **macOS** | ARM64 (M1/M2) | ✅ Full Support | Homebrew, Binary, Source |
| **Windows** | x86_64 | ✅ Full Support | Installer, Chocolatey, Source |
| **Docker** | Multi-arch | ✅ Full Support | Docker Hub |
| **Kubernetes** | Multi-arch | ✅ Full Support | Helm Charts |
| **WASM** | Browser | ✅ Full Support | npm, CDN |

### **System Requirements**

#### **Minimum Requirements**
- **RAM**: 512MB available memory
- **Storage**: 100MB free space for installation
- **CPU**: Single core (any modern architecture)
- **OS**: Any supported platform (see above)

#### **Recommended Requirements**
- **RAM**: 2GB+ for optimal performance
- **Storage**: 1GB+ free space for data and caching
- **CPU**: Multi-core for parallel operations
- **SSD**: Solid-state storage for best I/O performance

---

## 🚀 **Quick Installation**

### **One-Line Install (Linux/macOS)**
```bash
# Install latest CQLite with automatic platform detection
curl -sSL https://install.cqlite.dev | bash

# Verify installation
cqlite --version
```

### **Windows PowerShell**
```powershell
# Install using PowerShell
iwr https://install.cqlite.dev/windows.ps1 | iex

# Verify installation
cqlite --version
```

### **Docker Quick Start**
```bash
# Run CQLite in Docker
docker run -it --rm cqlite/cqlite:latest

# With persistent data
docker run -v $(pwd)/data:/data cqlite/cqlite:latest --database-path /data
```

---

## 🐧 **Linux Installation**

### **Option 1: Package Manager Installation**

#### **Ubuntu/Debian (APT)**
```bash
# Add CQLite repository
curl -fsSL https://pkg.cqlite.dev/gpg | sudo gpg --dearmor -o /usr/share/keyrings/cqlite.gpg
echo "deb [signed-by=/usr/share/keyrings/cqlite.gpg] https://pkg.cqlite.dev/apt stable main" | sudo tee /etc/apt/sources.list.d/cqlite.list

# Update package list and install
sudo apt update
sudo apt install cqlite

# Verify installation
cqlite --version
```

#### **CentOS/RHEL/Fedora (YUM/DNF)**
```bash
# Add CQLite repository
sudo tee /etc/yum.repos.d/cqlite.repo << EOF
[cqlite]
name=CQLite Repository
baseurl=https://pkg.cqlite.dev/yum/stable
enabled=1
gpgcheck=1
gpgkey=https://pkg.cqlite.dev/gpg
EOF

# Install CQLite
sudo dnf install cqlite  # Fedora
sudo yum install cqlite  # CentOS/RHEL

# Verify installation
cqlite --version
```

#### **Arch Linux (AUR)**
```bash
# Using yay
yay -S cqlite

# Using paru
paru -S cqlite

# Manual AUR build
git clone https://aur.archlinux.org/cqlite.git
cd cqlite
makepkg -si
```

### **Option 2: Binary Installation**
```bash
# Download latest binary
ARCH=$(uname -m)
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
wget https://github.com/pmcfadin/cqlite/releases/latest/download/cqlite-${OS}-${ARCH}.tar.gz

# Extract and install
tar -xzf cqlite-${OS}-${ARCH}.tar.gz
sudo mv cqlite /usr/local/bin/
sudo chmod +x /usr/local/bin/cqlite

# Verify installation
cqlite --version
```

### **Option 3: Snap Package**
```bash
# Install from Snap Store
sudo snap install cqlite

# Enable classic confinement for file system access
sudo snap connect cqlite:home

# Verify installation
cqlite --version
```

### **Option 4: AppImage (Portable)**
```bash
# Download AppImage
wget https://github.com/pmcfadin/cqlite/releases/latest/download/CQLite-x86_64.AppImage

# Make executable and run
chmod +x CQLite-x86_64.AppImage
./CQLite-x86_64.AppImage --version

# Optional: Integrate with desktop
./CQLite-x86_64.AppImage --appimage-extract
sudo mv squashfs-root /opt/cqlite
sudo ln -s /opt/cqlite/AppRun /usr/local/bin/cqlite
```

---

## 🍎 **macOS Installation**

### **Option 1: Homebrew (Recommended)**
```bash
# Install via Homebrew
brew tap pmcfadin/cqlite
brew install cqlite

# Upgrade to latest version
brew upgrade cqlite

# Verify installation
cqlite --version
```

### **Option 2: MacPorts**
```bash
# Install via MacPorts
sudo port install cqlite

# Verify installation
cqlite --version
```

### **Option 3: Binary Installation**
```bash
# Download for your architecture
# For Intel Macs:
wget https://github.com/pmcfadin/cqlite/releases/latest/download/cqlite-darwin-x86_64.tar.gz

# For Apple Silicon (M1/M2):
wget https://github.com/pmcfadin/cqlite/releases/latest/download/cqlite-darwin-arm64.tar.gz

# Extract and install
tar -xzf cqlite-darwin-*.tar.gz
sudo mv cqlite /usr/local/bin/
sudo chmod +x /usr/local/bin/cqlite

# Verify installation
cqlite --version
```

### **Option 4: DMG Installer**
```bash
# Download DMG file
wget https://github.com/pmcfadin/cqlite/releases/latest/download/CQLite.dmg

# Mount and install
hdiutil mount CQLite.dmg
sudo cp -R "/Volumes/CQLite/CQLite.app" /Applications/
hdiutil unmount "/Volumes/CQLite"

# Create command line symlink
sudo ln -s /Applications/CQLite.app/Contents/MacOS/cqlite /usr/local/bin/cqlite
```

---

## 🪟 **Windows Installation**

### **Option 1: Windows Installer (MSI)**
```powershell
# Download and run installer
$url = "https://github.com/pmcfadin/cqlite/releases/latest/download/CQLite-Setup.msi"
$output = "$env:TEMP\CQLite-Setup.msi"
Invoke-WebRequest -Uri $url -OutFile $output
Start-Process msiexec.exe -ArgumentList "/i", $output, "/quiet" -Wait

# Verify installation (restart shell first)
cqlite --version
```

### **Option 2: Chocolatey**
```powershell
# Install Chocolatey if not already installed
Set-ExecutionPolicy Bypass -Scope Process -Force
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))

# Install CQLite
choco install cqlite

# Verify installation
cqlite --version
```

### **Option 3: Scoop**
```powershell
# Install Scoop if not already installed
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
irm get.scoop.sh | iex

# Add CQLite bucket and install
scoop bucket add cqlite https://github.com/pmcfadin/scoop-cqlite
scoop install cqlite

# Verify installation
cqlite --version
```

### **Option 4: WinGet**
```powershell
# Install using Windows Package Manager
winget install pmcfadin.CQLite

# Verify installation
cqlite --version
```

### **Option 5: Portable ZIP**
```powershell
# Download portable ZIP
$url = "https://github.com/pmcfadin/cqlite/releases/latest/download/cqlite-windows-x86_64.zip"
$output = "$env:USERPROFILE\Downloads\cqlite-windows.zip"
Invoke-WebRequest -Uri $url -OutFile $output

# Extract to program files
Expand-Archive -Path $output -DestinationPath "C:\Program Files\CQLite"

# Add to PATH
$env:PATH += ";C:\Program Files\CQLite"
[Environment]::SetEnvironmentVariable("PATH", $env:PATH, "Machine")

# Verify installation (restart shell first)
cqlite --version
```

---

## 🐳 **Docker Installation**

### **Docker Hub Images**

#### **Official Images**
```bash
# Latest stable release
docker pull cqlite/cqlite:latest

# Specific version
docker pull cqlite/cqlite:v1.0.0

# Alpine-based (smaller image)
docker pull cqlite/cqlite:alpine

# Debian-based (full features)
docker pull cqlite/cqlite:debian
```

#### **Architecture-Specific Images**
```bash
# Multi-architecture support
docker pull cqlite/cqlite:latest         # Auto-detect architecture
docker pull cqlite/cqlite:amd64         # Intel/AMD 64-bit
docker pull cqlite/cqlite:arm64         # ARM 64-bit (Apple Silicon, ARM servers)
docker pull cqlite/cqlite:armv7         # ARM 32-bit (Raspberry Pi)
```

### **Docker Usage Examples**

#### **Basic Usage**
```bash
# Run CQLite interactively
docker run -it --rm cqlite/cqlite:latest

# Run with persistent data volume
docker run -it --rm \
  -v $(pwd)/data:/data \
  cqlite/cqlite:latest \
  --database-path /data

# Run as daemon with port exposure
docker run -d \
  --name cqlite-server \
  -p 8080:8080 \
  -v cqlite-data:/data \
  cqlite/cqlite:latest \
  server --host 0.0.0.0 --port 8080 --database-path /data
```

#### **Docker Compose**
```yaml
# docker-compose.yml
version: '3.8'

services:
  cqlite:
    image: cqlite/cqlite:latest
    container_name: cqlite
    ports:
      - "8080:8080"
    volumes:
      - cqlite-data:/data
      - ./config:/config:ro
    environment:
      - CQLITE_DATABASE_PATH=/data
      - CQLITE_CONFIG_FILE=/config/cqlite.yaml
      - CQLITE_LOG_LEVEL=info
    command: ["server", "--host", "0.0.0.0", "--port", "8080"]
    restart: unless-stopped

volumes:
  cqlite-data:
    driver: local
```

#### **Development Environment**
```bash
# Run development environment with hot reload
docker run -it --rm \
  -v $(pwd):/workspace \
  -v cqlite-dev-data:/data \
  -p 8080:8080 \
  cqlite/cqlite:dev \
  --database-path /data \
  --watch /workspace
```

---

## ☸️ **Kubernetes Installation**

### **Helm Chart Installation**

#### **Add Helm Repository**
```bash
# Add CQLite Helm repository
helm repo add cqlite https://charts.cqlite.dev
helm repo update

# Search available charts
helm search repo cqlite
```

#### **Basic Installation**
```bash
# Install with default values
helm install my-cqlite cqlite/cqlite

# Install with custom values
helm install my-cqlite cqlite/cqlite \
  --set persistence.size=100Gi \
  --set resources.requests.memory=2Gi \
  --set service.type=LoadBalancer
```

#### **Custom Values File**
```yaml
# values.yaml
replicaCount: 3

image:
  repository: cqlite/cqlite
  tag: "latest"
  pullPolicy: IfNotPresent

service:
  type: ClusterIP
  port: 8080

persistence:
  enabled: true
  size: 50Gi
  storageClass: "fast-ssd"

resources:
  requests:
    memory: "1Gi"
    cpu: "500m"
  limits:
    memory: "2Gi"
    cpu: "1000m"

config:
  database_path: "/data"
  cache_size: "512MB"
  compression: "lz4"
  performance_mode: true

ingress:
  enabled: true
  className: "nginx"
  hosts:
    - host: cqlite.example.com
      paths:
        - path: /
          pathType: Prefix

monitoring:
  enabled: true
  serviceMonitor:
    enabled: true
```

```bash
# Install with custom values
helm install my-cqlite cqlite/cqlite -f values.yaml
```

### **Manual Kubernetes Deployment**

#### **Namespace and ConfigMap**
```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: cqlite
---
# configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cqlite-config
  namespace: cqlite
data:
  cqlite.yaml: |
    database_path: "/data"
    cache_size: "512MB"
    compression: "lz4"
    performance_mode: true
    server:
      host: "0.0.0.0"
      port: 8080
    logging:
      level: "info"
      format: "json"
```

#### **StatefulSet Deployment**
```yaml
# statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: cqlite
  namespace: cqlite
spec:
  serviceName: cqlite
  replicas: 3
  selector:
    matchLabels:
      app: cqlite
  template:
    metadata:
      labels:
        app: cqlite
    spec:
      containers:
      - name: cqlite
        image: cqlite/cqlite:latest
        ports:
        - containerPort: 8080
          name: http
        env:
        - name: CQLITE_CONFIG_FILE
          value: "/config/cqlite.yaml"
        volumeMounts:
        - name: data
          mountPath: /data
        - name: config
          mountPath: /config
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: config
        configMap:
          name: cqlite-config
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: "fast-ssd"
      resources:
        requests:
          storage: 50Gi
```

#### **Service and Ingress**
```yaml
# service.yaml
apiVersion: v1
kind: Service
metadata:
  name: cqlite
  namespace: cqlite
spec:
  selector:
    app: cqlite
  ports:
  - port: 8080
    targetPort: 8080
    name: http
  type: ClusterIP
---
# ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: cqlite
  namespace: cqlite
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /
spec:
  ingressClassName: nginx
  rules:
  - host: cqlite.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: cqlite
            port:
              number: 8080
```

#### **Deploy All Resources**
```bash
# Apply all Kubernetes resources
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f statefulset.yaml
kubectl apply -f service.yaml
kubectl apply -f ingress.yaml

# Verify deployment
kubectl get pods -n cqlite
kubectl get svc -n cqlite
kubectl logs -f statefulset/cqlite -n cqlite
```

---

## 🌐 **Browser/WASM Installation**

### **NPM Package**
```bash
# Install CQLite WASM package
npm install cqlite-wasm

# Or with Yarn
yarn add cqlite-wasm
```

#### **JavaScript/TypeScript Usage**
```typescript
// Import CQLite WASM module
import init, { Database } from 'cqlite-wasm';

// Initialize WASM module
await init();

// Create database instance
const db = new Database('my-database', {
  maxMemory: 50 * 1024 * 1024, // 50MB
  cacheSize: 10 * 1024 * 1024, // 10MB cache
});

await db.open();

// Use database
const results = await db.select('SELECT * FROM users');
console.log(results);
```

### **CDN Usage**
```html
<!DOCTYPE html>
<html>
<head>
    <title>CQLite WASM Example</title>
</head>
<body>
    <script type="module">
        // Load from CDN
        import init, { Database } from 'https://cdn.jsdelivr.net/npm/cqlite-wasm@latest/dist/cqlite.js';
        
        async function main() {
            // Initialize WASM
            await init();
            
            // Create database
            const db = new Database('demo-db');
            await db.open();
            
            // Create table
            await db.execute(`
                CREATE TABLE users (
                    id UUID PRIMARY KEY,
                    name TEXT,
                    email TEXT
                )
            `);
            
            // Insert data
            await db.execute(`
                INSERT INTO users (id, name, email) 
                VALUES (?, ?, ?)
            `, [crypto.randomUUID(), 'John Doe', 'john@example.com']);
            
            // Query data
            const results = await db.select('SELECT * FROM users');
            console.log('Users:', results);
        }
        
        main().catch(console.error);
    </script>
</body>
</html>
```

### **React Integration**
```typescript
// components/CQLiteProvider.tsx
import React, { createContext, useContext, useEffect, useState } from 'react';
import init, { Database } from 'cqlite-wasm';

interface CQLiteContextType {
  db: Database | null;
  isReady: boolean;
}

const CQLiteContext = createContext<CQLiteContextType>({
  db: null,
  isReady: false,
});

export const CQLiteProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [db, setDb] = useState<Database | null>(null);
  const [isReady, setIsReady] = useState(false);

  useEffect(() => {
    const initCQLite = async () => {
      try {
        await init();
        const database = new Database('app-database');
        await database.open();
        setDb(database);
        setIsReady(true);
      } catch (error) {
        console.error('Failed to initialize CQLite:', error);
      }
    };

    initCQLite();
  }, []);

  return (
    <CQLiteContext.Provider value={{ db, isReady }}>
      {children}
    </CQLiteContext.Provider>
  );
};

export const useCQLite = () => {
  const context = useContext(CQLiteContext);
  if (!context) {
    throw new Error('useCQLite must be used within a CQLiteProvider');
  }
  return context;
};
```

---

## 🔧 **Configuration**

### **Configuration File (cqlite.yaml)**
```yaml
# Complete CQLite configuration example
database:
  # Path to database files
  path: "/data/cqlite"
  
  # Database creation settings
  create_if_missing: true
  error_if_exists: false
  
  # Performance settings
  cache_size: "512MB"
  write_buffer_size: "64MB"
  compression: "lz4"  # Options: lz4, snappy, deflate, none
  compression_level: 6
  
  # Concurrency settings
  max_open_files: 1000
  parallel_workers: 8
  
  # Memory management
  memory_limit: "2GB"
  streaming_threshold: "100MB"
  use_mmap: true

# Server configuration (for daemon mode)
server:
  host: "127.0.0.1"
  port: 8080
  
  # TLS configuration
  tls:
    enabled: false
    cert_file: "/etc/cqlite/server.crt"
    key_file: "/etc/cqlite/server.key"
  
  # CORS settings
  cors:
    enabled: true
    allowed_origins: ["*"]
    allowed_methods: ["GET", "POST", "PUT", "DELETE"]
    allowed_headers: ["Content-Type", "Authorization"]

# Logging configuration
logging:
  level: "info"  # Options: trace, debug, info, warn, error
  format: "text"  # Options: text, json
  output: "stdout"  # Options: stdout, stderr, file
  file: "/var/log/cqlite/cqlite.log"
  
  # Log rotation
  rotation:
    enabled: true
    max_size: "100MB"
    max_files: 10
    max_age: "30d"

# Monitoring and metrics
monitoring:
  enabled: true
  prometheus:
    enabled: true
    path: "/metrics"
    port: 9090
  
  health_check:
    enabled: true
    path: "/health"
    interval: "30s"

# Security settings
security:
  # Authentication
  auth:
    enabled: false
    type: "basic"  # Options: basic, jwt, oauth2
    config:
      username: "admin"
      password: "secure_password"
  
  # Rate limiting
  rate_limit:
    enabled: true
    requests_per_second: 1000
    burst_size: 5000

# Backup configuration
backup:
  enabled: true
  schedule: "0 2 * * *"  # Daily at 2 AM
  destination: "/backup/cqlite"
  retention: "30d"
  compression: true

# Advanced settings
advanced:
  # Cassandra compatibility
  cassandra_compatibility: true
  
  # Experimental features
  experimental:
    wasm_support: true
    analytics_engine: true
    ml_integration: false
  
  # Performance tuning
  tuning:
    bloom_filter_bits: 10
    index_cache_size: "256MB"
    block_cache_size: "1GB"
    prefetch_enabled: true
```

### **Environment Variables**
```bash
# Core configuration
export CQLITE_DATABASE_PATH="/data/cqlite"
export CQLITE_CACHE_SIZE="512MB"
export CQLITE_COMPRESSION="lz4"
export CQLITE_PERFORMANCE_MODE="true"

# Server configuration
export CQLITE_SERVER_HOST="0.0.0.0"
export CQLITE_SERVER_PORT="8080"
export CQLITE_TLS_ENABLED="false"

# Logging
export CQLITE_LOG_LEVEL="info"
export CQLITE_LOG_FORMAT="json"
export CQLITE_LOG_FILE="/var/log/cqlite/cqlite.log"

# Security
export CQLITE_AUTH_ENABLED="false"
export CQLITE_AUTH_USERNAME="admin"
export CQLITE_AUTH_PASSWORD="secure_password"

# Monitoring
export CQLITE_METRICS_ENABLED="true"
export CQLITE_PROMETHEUS_PORT="9090"
export CQLITE_HEALTH_CHECK_ENABLED="true"

# Backup
export CQLITE_BACKUP_ENABLED="true"
export CQLITE_BACKUP_SCHEDULE="0 2 * * *"
export CQLITE_BACKUP_DESTINATION="/backup/cqlite"
```

### **Command Line Options**
```bash
# Display all available options
cqlite --help

# Common usage patterns
cqlite --database-path /data/cqlite \
        --cache-size 1GB \
        --compression lz4 \
        --performance-mode \
        --log-level info

# Server mode
cqlite server --host 0.0.0.0 \
              --port 8080 \
              --database-path /data/cqlite \
              --config /etc/cqlite/config.yaml

# Interactive mode
cqlite interactive --database-path /data/cqlite

# Batch operations
cqlite batch --input-file queries.sql \
             --output-file results.json \
             --database-path /data/cqlite
```

---

## 🔐 **Security Configuration**

### **Authentication Setup**

#### **Basic Authentication**
```yaml
# config.yaml
security:
  auth:
    enabled: true
    type: "basic"
    config:
      username: "admin"
      password: "secure_password_here"
      
      # Password hashing
      hash_algorithm: "bcrypt"
      rounds: 12
```

#### **JWT Authentication**
```yaml
security:
  auth:
    enabled: true
    type: "jwt"
    config:
      secret_key: "your-secret-key-here"
      token_expiry: "24h"
      issuer: "cqlite-server"
      audience: "cqlite-clients"
```

#### **OAuth2 Integration**
```yaml
security:
  auth:
    enabled: true
    type: "oauth2"
    config:
      provider: "auth0"  # or "google", "github", etc.
      client_id: "your-client-id"
      client_secret: "your-client-secret"
      redirect_uri: "https://your-app.com/callback"
      scopes: ["read", "write"]
```

### **TLS/SSL Configuration**
```yaml
server:
  tls:
    enabled: true
    cert_file: "/etc/cqlite/tls/server.crt"
    key_file: "/etc/cqlite/tls/server.key"
    ca_file: "/etc/cqlite/tls/ca.crt"
    
    # TLS settings
    min_version: "1.2"
    cipher_suites:
      - "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
      - "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
    
    # Client certificate verification
    client_auth: "optional"  # none, optional, required
```

#### **Generate Self-Signed Certificates**
```bash
# Create certificate directory
sudo mkdir -p /etc/cqlite/tls
cd /etc/cqlite/tls

# Generate private key
openssl genrsa -out server.key 2048

# Generate certificate signing request
openssl req -new -key server.key -out server.csr -subj "/CN=cqlite-server"

# Generate self-signed certificate
openssl x509 -req -in server.csr -signkey server.key -out server.crt -days 365

# Set proper permissions
sudo chmod 600 server.key
sudo chmod 644 server.crt
sudo chown cqlite:cqlite server.*
```

### **Firewall Configuration**

#### **UFW (Ubuntu Firewall)**
```bash
# Allow CQLite server port
sudo ufw allow 8080/tcp comment "CQLite Server"

# Allow Prometheus metrics port
sudo ufw allow 9090/tcp comment "CQLite Metrics"

# Allow from specific IP ranges only
sudo ufw allow from 10.0.0.0/8 to any port 8080
sudo ufw allow from 192.168.0.0/16 to any port 8080
```

#### **iptables**
```bash
# Allow CQLite ports
sudo iptables -A INPUT -p tcp --dport 8080 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 9090 -j ACCEPT

# Save rules
sudo iptables-save > /etc/iptables/rules.v4
```

---

## 📊 **Monitoring Setup**

### **Prometheus Integration**
```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'cqlite'
    static_configs:
      - targets: ['localhost:9090']
    scrape_interval: 10s
    metrics_path: /metrics
```

### **Grafana Dashboard**
```json
{
  "dashboard": {
    "title": "CQLite Monitoring",
    "panels": [
      {
        "title": "Query Performance",
        "type": "graph",
        "targets": [
          {
            "expr": "cqlite_query_duration_seconds",
            "legend": "Query Duration"
          }
        ]
      },
      {
        "title": "Memory Usage",
        "type": "singlestat",
        "targets": [
          {
            "expr": "cqlite_memory_usage_bytes / 1024 / 1024",
            "legend": "Memory Usage (MB)"
          }
        ]
      }
    ]
  }
}
```

### **Health Checks**
```bash
# HTTP health check
curl -f http://localhost:8080/health || exit 1

# Detailed health information
curl http://localhost:8080/health/detailed

# Example response:
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "checks": {
    "database": "healthy",
    "memory": "healthy",
    "disk_space": "healthy"
  },
  "metrics": {
    "uptime_seconds": 3600,
    "memory_usage_mb": 512,
    "disk_usage_gb": 2.5
  }
}
```

---

## 🗂️ **Data Directory Structure**

### **Default Directory Layout**
```
/data/cqlite/
├── config/
│   ├── cqlite.yaml              # Main configuration
│   └── schemas/                 # Schema definitions
├── data/
│   ├── keyspace1/
│   │   ├── table1-Data.db       # Table data files
│   │   ├── table1-Index.db      # Index files
│   │   └── table1-Statistics.db # Statistics
│   └── keyspace2/
├── logs/
│   ├── cqlite.log              # Application logs
│   ├── access.log              # Access logs
│   └── error.log               # Error logs
├── backup/
│   ├── daily/                  # Daily backups
│   ├── weekly/                 # Weekly backups
│   └── snapshots/              # Manual snapshots
├── cache/
│   ├── index_cache/            # Index cache files
│   └── query_cache/            # Query result cache
└── tmp/
    ├── import/                 # Temporary import files
    └── export/                 # Temporary export files
```

### **Permissions Setup**
```bash
# Create CQLite user and group
sudo groupadd cqlite
sudo useradd -r -g cqlite -s /bin/false cqlite

# Create directories with proper permissions
sudo mkdir -p /data/cqlite/{config,data,logs,backup,cache,tmp}
sudo chown -R cqlite:cqlite /data/cqlite
sudo chmod -R 755 /data/cqlite
sudo chmod -R 700 /data/cqlite/config
sudo chmod -R 640 /data/cqlite/logs
```

---

## 🔄 **Backup and Recovery**

### **Automated Backup Setup**
```bash
# Create backup script
sudo tee /usr/local/bin/cqlite-backup.sh << 'EOF'
#!/bin/bash
set -e

BACKUP_DIR="/backup/cqlite"
DATA_DIR="/data/cqlite"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_NAME="cqlite_backup_${TIMESTAMP}"

# Create backup directory
mkdir -p "${BACKUP_DIR}/${BACKUP_NAME}"

# Stop CQLite service (optional, for consistent backup)
# systemctl stop cqlite

# Create backup
cqlite backup create \
  --source-path "${DATA_DIR}" \
  --destination-path "${BACKUP_DIR}/${BACKUP_NAME}" \
  --compression lz4 \
  --verify-integrity

# Restart CQLite service
# systemctl start cqlite

# Clean old backups (keep last 30 days)
find "${BACKUP_DIR}" -type d -name "cqlite_backup_*" -mtime +30 -exec rm -rf {} +

echo "Backup completed: ${BACKUP_DIR}/${BACKUP_NAME}"
EOF

sudo chmod +x /usr/local/bin/cqlite-backup.sh

# Add to crontab
echo "0 2 * * * /usr/local/bin/cqlite-backup.sh" | sudo crontab -u cqlite -
```

### **Recovery Procedures**
```bash
# List available backups
cqlite backup list --backup-dir /backup/cqlite

# Restore from backup
cqlite backup restore \
  --backup-path "/backup/cqlite/cqlite_backup_20240115_020000" \
  --destination-path "/data/cqlite" \
  --verify-integrity \
  --force

# Partial restore (specific keyspace)
cqlite backup restore \
  --backup-path "/backup/cqlite/cqlite_backup_20240115_020000" \
  --destination-path "/data/cqlite" \
  --keyspace production_data \
  --verify-integrity
```

---

## 🔧 **Troubleshooting**

### **Common Installation Issues**

#### **Permission Denied Errors**
```bash
# Fix permission issues
sudo chown -R $USER:$USER ~/.cqlite
sudo chmod -R 755 ~/.cqlite

# For system-wide installation
sudo chown -R cqlite:cqlite /data/cqlite
sudo chmod -R 755 /data/cqlite
```

#### **Library Dependencies (Linux)**
```bash
# Install missing dependencies on Ubuntu/Debian
sudo apt update
sudo apt install -y libc6 libgcc1 libssl1.1

# Install missing dependencies on CentOS/RHEL
sudo yum install -y glibc libgcc openssl-libs

# Check library dependencies
ldd $(which cqlite)
```

#### **Path Issues**
```bash
# Add CQLite to PATH permanently
echo 'export PATH="/usr/local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

# Verify PATH
which cqlite
cqlite --version
```

### **Performance Issues**

#### **Memory Usage**
```bash
# Monitor memory usage
cqlite status --show-memory

# Adjust cache settings
cqlite config set cache_size 1GB
cqlite config set memory_limit 2GB
```

#### **Disk I/O**
```bash
# Check disk usage
cqlite status --show-disk

# Monitor I/O performance
iostat -x 1 10

# Optimize for SSD
cqlite config set use_mmap true
cqlite config set prefetch_enabled true
```

### **Getting Help**

#### **Debug Information**
```bash
# Generate debug report
cqlite debug report --output debug-report.zip

# Check system compatibility
cqlite debug system-check

# Validate configuration
cqlite config validate
```

#### **Support Resources**
- 📚 **Documentation**: https://docs.cqlite.dev
- 💬 **Community Forum**: https://discuss.cqlite.dev
- 🐛 **Bug Reports**: https://github.com/pmcfadin/cqlite/issues
- 📧 **Support Email**: support@cqlite.dev

---

## 🎯 **Next Steps**

### **Post-Installation Checklist**
- [ ] **Verify Installation**: Run `cqlite --version`
- [ ] **Configure Database Path**: Set up data directory
- [ ] **Test Basic Operations**: Create table and insert data
- [ ] **Configure Monitoring**: Set up health checks
- [ ] **Setup Backups**: Configure automated backups
- [ ] **Security Review**: Enable authentication if needed
- [ ] **Performance Tuning**: Optimize for your workload

### **Getting Started**
1. **Read the Quick Start Guide**: [QUICK_START.md](guides/QUICK_START.md)
2. **Try the Tutorial**: [TUTORIAL.md](guides/TUTORIAL.md)
3. **Explore Examples**: [examples/](examples/)
4. **Join the Community**: [Community Resources](#support-resources)

### **Advanced Topics**
- **Migration from Cassandra**: [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)
- **Performance Optimization**: [PERFORMANCE_GUIDE.md](PERFORMANCE_GUIDE.md)
- **API Documentation**: [API_SPECIFICATION.md](API_SPECIFICATION.md)
- **Architecture Deep Dive**: [ARCHITECTURE.md](ARCHITECTURE.md)

---

## 🎉 **Conclusion**

CQLite installation is designed to be **simple, fast, and reliable** across all platforms. Whether you're installing on a development laptop or deploying to a production Kubernetes cluster, CQLite provides consistent, high-performance database access with minimal operational overhead.

**Ready to get started?** Choose your platform above and follow the installation instructions to begin using CQLite in minutes!

---

*Generated by CompatibilityDocumenter Agent - CQLite Compatibility Swarm*
*Last Updated: 2025-07-16*
*Version: 1.0.0 - Complete Installation Guide*