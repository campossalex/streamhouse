# Streamhouse Hands-on

The Streamhouse workshop on open source only — **Apache Flink 1.20**, **Apache
Fluss 0.9.1** and **Apache Iceberg 1.10**, with Lakekeeper, MinIO, Trino, Kafka
(Redpanda), PostgreSQL and Grafana — all in one Docker Compose stack.

![](docs/streamhouse_diagram.png)

This page is how to deploy it. Once it is up:

- [WORKSHOP.md](WORKSHOP.md) — the attendee walkthrough, Lab 1 to Lab 5.
- [docs/REFERENCE.md](docs/REFERENCE.md) — architecture, how each piece is wired and why,
  running steps from a terminal, troubleshooting.

---

## Requirements

| | Minimum | Notes |
|---|---|---|
| **Memory for Docker** | **10 GB** | About 7–8 GB in use with every lab job running. On Docker Desktop, set it under *Settings → Resources*; on Linux, Docker uses the host's memory, so the machine needs ~16 GB. |
| **CPU** | 4 cores | Tested with 12. |
| **Disk** | 20 GB free | About 11 GB of images and 270 MB of JARs on the first run, plus data. |
| **Docker** | Docker Engine with **Compose v2 ≥ 2.20** | `docker compose version` to check. |
| **Tools** | `bash`, `curl`, `jq`, `git` | `start.sh` uses `curl` and `jq`. |
| **Internet access** | On the first run | Images from Docker Hub and quay.io, JARs from Maven Central, Python packages from PyPI. |
| **Free ports** | 80, 3000, 5432, 5433, 8080–8084, 8181, 8978, 9000, 9001, 9123, 19092 | Stop any other Docker Compose stack using them first. |

Images exist for both x86_64 and arm64 (Apple Silicon, AWS Graviton); the stack has been
tested on Apple Silicon and is built for x86_64 EC2 instances.

---

## Run locally

```bash
git clone https://github.com/campossalex/streaming_lakehouse.git
cd streaming_lakehouse

./start.sh --services-only
```

Then open **http://localhost** — the environment homepage, with a link to every tool.

`start.sh` downloads the JARs on first run, starts the stack and waits until every
service is healthy, then prints the URLs and a runbook of manual commands. The two modes:

| Command | Result | Use it for |
|---|---|---|
| `./start.sh --services-only` | Everything running, nothing submitted; the tiering service deployed but not started | **A workshop** — attendees start from Lab 1 |
| `./start.sh` | Also submits every lab job and starts tiering | **A demo** — the finished state of all labs |
| add `--reset` to either | Wipes every volume first: Fluss, the lake, Kafka, PostgreSQL | **Handing a clean environment** to the next group |

To stop:

```bash
docker compose down -v      # stop everything and delete its data
```

`./start.sh --reset` is the only clean restart — see
[docs/REFERENCE.md → Teardown](docs/REFERENCE.md#teardown) for why a plain `down`/`up` is not.

---

## Run on AWS

The same stack on a single EC2 instance. Attendees reach it through their browser; nothing
is installed on their machines.

> **Not tested end-to-end on AWS.** Everything below follows from how the stack is wired
> (see *Network* for the specifics), but a dry run before a live workshop is worth it.

### 1. Launch an instance

| Setting | Value |
|---|---|
| **Instance type** | `m6i.xlarge` (4 vCPU, 16 GB) at minimum; `m6i.2xlarge` (8 vCPU, 32 GB) for headroom |
| **AMI** | Amazon Linux 2023, x86_64 — the same family the repository's `terraform/` provisions |
| **Storage** | 40 GB gp3 root volume |
| **Network** | A public subnet with a public IPv4 address (or an Elastic IP, to keep the URL stable) |
| **Security group** | See [Network](#3-network) below |

Burstable `t3` types are not recommended: Flink, Trino and Fluss keep the CPU busy the
whole time, and once CPU credits run out the jobs slow to a crawl.

### 2. Install Docker and start the stack

On Amazon Linux 2023:

```bash
sudo dnf install -y docker git jq
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user

# Docker Compose v2 is not packaged for Amazon Linux 2023: install the CLI plugin
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -fsSL -o /usr/local/lib/docker/cli-plugins/docker-compose \
  https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
```

Log out and back in (for the `docker` group), then:

```bash
docker compose version      # must be ≥ 2.20

git clone https://github.com/campossalex/streaming_lakehouse.git
cd streaming_lakehouse
./start.sh --services-only
```

On Ubuntu, `curl -fsSL https://get.docker.com | sudo sh` installs Docker with the
Compose plugin in one step.

No clone access on the instance? Copy the directory instead:
`rsync -av --exclude lib/ --exclude .git/ ./ ec2-user@<host>:streaming_lakehouse/`
(`start.sh` downloads `lib/` on the instance).

### 3. Network

**Inbound — the security group.** Nothing in this stack has authentication: Grafana runs
as anonymous admin, the Flink Web UI accepts JAR uploads and runs them (arbitrary code),
the SQL Gateway executes any SQL, and MinIO uses default keys. **Never open these ports
to `0.0.0.0/0`.** Allow them only from your own IP, or the venue's egress range:

| Port | Service | Open to |
|---|---|---|
| 22 | SSH | Your IP |
| 80 | Environment homepage | Attendees |
| 8084 | Flink SQL editor | Attendees |
| 8081 | Flink Web UI | Attendees |
| 8978 | CloudBeaver (Trino) | Attendees |
| 8181 | Lakekeeper UI | Attendees |
| 9001 | MinIO Console | Attendees |
| 8082 | Kafka Console | Attendees |
| 3000 | Grafana | Attendees |
| 8080 | Trino Web UI | Attendees (optional) |
| 5432, 5433, 8083, 9000, 9123, 19092 | PostgreSQL (shop, dwh), SQL Gateway, S3 API, Fluss, Kafka | **Nobody** — the containers reach each other on the internal Docker network; these are only for your own clients |

**Outbound.** The instance needs internet access on the first `./start.sh` (and on every
`--reset`, for the Python packages): Docker Hub, `quay.io`, Maven Central, PyPI,
GitHub. In a private subnet that means a NAT gateway.

**How the URLs work.** The homepage and the SQL editor build every link from the address
you opened them with, so `http://<public-ip>` or `http://<public-dns>` just works — no
configuration. Share **`http://<public-ip-or-dns>`** with attendees.

**Kafka from outside the instance does not work as-is.** Redpanda advertises its external
listener as `localhost:19092`, so a Kafka client on another machine connects and is then
redirected to its own localhost. This does not affect the labs (every client runs inside
the Docker network). To produce from outside, change `external://localhost:19092` in
`--advertise-kafka-addr` (`docker-compose.yml`, service `redpanda`) to the instance's
public DNS name, and open 19092 to that client only.

**No public ports at all?** Keep everything closed except SSH and use a tunnel from your
laptop; then use the same `localhost` URLs as locally (port 80 needs `sudo` on your side):

```bash
sudo ssh -N -i key.pem ec2-user@<host> \
  -L 80:localhost:80 -L 8084:localhost:8084 -L 8081:localhost:8081 -L 8978:localhost:8978 \
  -L 8181:localhost:8181 -L 9001:localhost:9001 -L 8082:localhost:8082 -L 3000:localhost:3000
```

### 4. Clean up

`docker compose down -v` on the instance, then stop or terminate it — an `m6i.xlarge` left
running costs roughly $4–6 a day, depending on the region.

---

## Services and endpoints

Replace `localhost` with the instance's address when running on AWS.

### Web UIs

| Service | URL | Credentials | Used in |
|---|---|---|---|
| **Environment homepage** — links to everything below, with live status | http://localhost | — | Start here |
| **Flink SQL editor** | http://localhost:8084 | — | Labs 1–5 |
| Flink Web UI | http://localhost:8081 | — | Labs 1–5 |
| Kafka Console (Redpanda Console) | http://localhost:8082 | — | Lab 1 |
| CloudBeaver (Trino) | http://localhost:8978 | anonymous; admin `cbadmin` / `Admin123` | Labs 3–4 |
| Lakekeeper UI | http://localhost:8181/ui | — | Lab 3 |
| MinIO Console | http://localhost:9001 | `admin` / `password` | Lab 3 |
| Trino Web UI | http://localhost:8080 | any username | Lab 4 |
| Grafana — *Order Analytics* | http://localhost:3000 | anonymous admin | Lab 5 |

### Data endpoints

| Endpoint | From the host | Inside the Docker network (what the lab SQL uses) | Credentials |
|---|---|---|---|
| Kafka | `localhost:19092` | `redpanda:9092` | — |
| Fluss coordinator | `localhost:9123` | `coordinator-server:9123` | — |
| PostgreSQL — the shop's source database `shop` (`product_catalog`) | `localhost:5432` | `postgres:5432` | `shop_user` / `admin1`; CDC user `cdc_user` / `admin1` |
| PostgreSQL — the data warehouse `dwh` (`revenue_1m`) | `localhost:5433` | `postgres-dwh:5432` | `dwh_user` / `admin1` |
| Iceberg REST catalog | `http://localhost:8181/catalog` | `http://lakekeeper:8181/catalog` | warehouse `lakehouse` |
| MinIO S3 API | `http://localhost:9000` | `http://minio:9000` | `admin` / `password` |
| Flink SQL Gateway (REST) | `http://localhost:8083` | `http://sql-gateway:8083` | — |
