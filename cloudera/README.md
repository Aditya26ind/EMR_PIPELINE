## Cloudera QuickStart (single-node) via Docker

This repo lets you spin up a single-node Cloudera QuickStart-style image (CDH 6.3.2, schema2) on your machine for local exploration. It is **not** a production deployment; modern CDP deployments require licensed installers and multi-node resources.

### Prerequisites
- Docker (≥ 20.x) and enough RAM/CPU (≥ 8 GB RAM and 4 vCPU recommended).
- Network access to pull `cloudera/quickstart:latest` from Docker Hub the first time you build.

### Build and run
```bash
# Start the container (pulls the prebuilt schema2 image)
docker compose up
```

The helper script `/usr/bin/docker-quickstart` (set as the default command) initializes and starts HDFS, YARN, Hive, Impala, Hue, and related daemons inside the container.

### Accessing services
- Hue UI: http://localhost:8888 (user: `cloudera`, password: `cloudera`)
- HDFS NameNode UI: http://localhost:50070
- HiveServer2: `localhost:10000`
- Impala JDBC/ODBC: `localhost:21050`
- Cloudera Manager (optional; heavy): start it with `docker exec -it cloudera-quickstart service cloudera-scm-server start` then open http://localhost:7180 (user/pass: `admin`/`admin` after first login).

### Notes and limitations
- QuickStart is frozen on CDH 6.x (schema2 image); expect outdated components and security baselines.
- Running Hadoop daemons in one container is resource-intensive; monitor local CPU/memory.
- For modern CDP Public Cloud or Private Cloud Base, obtain the official parcels and deployment tooling from Cloudera Support—those are not legally redistributable here.
- Apple Silicon/arm64 users: this image is amd64-only, so Docker Desktop will use emulation; the first download/convert can take a while.
