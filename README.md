# Introduction

**VECROsim** is a microservice fault simulation and diagnosis benchmarking platform. It supports:
- Deploying configurable service topologies in Kubernetes
- Injecting multiple types of faults (CPU, memory, network, I/O, pod) using Fault Injector built inside or Chaos Mesh.
- Collecting **multi-modal observability data** (metrics, logs, and traces) aligned with fault events.
- Generating benchmark datasets for resilience evaluation and root cause diagnosis research

Compared to [VECROsim]([etigerstudio/VECROsim](https://github.com/etigerstudio/VECROsim)), VECROsim integrates distributed tracing and logging, and provides more fine-grained, declarative fault injection capabilities.

## ✨ Features

- **Configurable service topologies**: define services and dependencies via YAML
- **Rich fault types**: network delay/loss/partition, CPU hog, memory pressure, I/O latency/faults, pod failures
- **Chaos Mesh integration**: declarative, scheduled, and visualized fault injection
- **Multi-modal observability**: Prometheus metrics, structured logs, and Jaeger traces
- **Dataset export**: easy to build a collection of fault-aware datasets for anomaly detection and diagnosis tasks

# Quick Start Guide

## 📦 Prerequisites

- A running **Kubernetes cluster**
- `kubectl` and `docker`
- Go 1.19+
- Python 3.7+
- (Optional) Chaos Mesh, Prometheus Operator, Jaeger
- Active Internet connection (possibly need for pulling images).

## Install Monitoring Infrastructure

Currently VECROsim does not support automatically install the `kube-prometheus` monitoring infrastructure. Execute the following command to setup the monitoring infrastructure.

```
./observability/metrics/setup/setup.sh   # Prometheus, Grafana, Alertmanager, jaeger-exporter
```

## Build VECROsim Container Images

Build and upload `vecro-base` and `vecro-mongodb`:

```shell
cd container_images/vecro-base
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o vecro-base .
docker build -t vecro-base:v1 # Use proper docker enviroment to build and upload the image
```

```shell
cd ../vecro-mongodb
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o vecro-mongodb .
docker build -t vecro-mongodb:v1 # Use proper docker enviroment to build and upload the image
```

## Deploy Microservice System

Create a `Kubernetes` namespace and deploy the `Social` microservice system onto it:

```shell
cd deploy
kubectl create namespace social # Create the k8s namespace for Social to deploy on
go run . -deffile base/social.yaml # Deploy Social system in the cluster
```

Create the service monitor for `social` namespace to enable metrics collecting:

```shell
kubectl apply -f base/social-monitor.yaml # Install Prometheus monitor for Social system
```

## Apply User-side Load

Expose the `text` front-end service and apply a 2-hour load to the service.

```shell
kubectl port-forward -n social svc/social-text 8080:80 > /dev/null & # Make 'text' service accessible at http://localhost:8080
cd ./VECROSim/load
go run . -delay 100ms -duration 2h -users 5 -url "http://localhost:8080
```

## Inject Faults

Inject the a `network delay` fault (`3 minutes` `500ms` network delay to service `posts-storage`) to the system.

```shell
kubectl apply -f ./VECROSim/inject/social-delay.yaml # Inject network delay fault to the deployed system
```

## Generate Datasets

Now you could generate CSV file datasets. 

```shell
cd observability
python ./collect_metrics.py # Use python script to communicate with Prometheus to download and generate dataset
./collect_logs.sh # Collect and export logs from the logging pipeline
./collect_traces.sh # Collect and export distributed traces from Jaeger
```

You may need configure the timestamps of collection range in the script first, please refer to Command Manual for details

# Source Code Structure

## VECROsim

- `deploy`: the service deployer module.
- `inject`: the fault injector module. 2 example fault configuration are also included.
- `load`: the user-side load generator module. 
- `observability`: the observability infrastructure setup and collector scripts. 

## Container Images

- `vecro-base`: the image for logic services.
- `vecro-mongodb`: the image for concrete service MongoDB. 

# Command Manual

## deploy

`deploy` command deploys a microservice `system definition`(see Configuration Reference) in  `deffile`. Use `kubeconfig` argument to specify config file for `kubectl` manually.

All arguments of `deploy`:

```shell
-deffile string
    	path to system definition file
-kubeconfig string
    	(optional) absolute path to the kubeconfig file (default "~/.kube/config")
```

This command is built in `Go`, and to run it you could either run `go build` to first build the executable binary or `go run` to directly build and run the command. 

Example:

```shell
./deploy -deffile your-system.yaml
```

## inject

`inject` command inject a fault defined in `fault definition`(see Configuration Reference) in  `deffile`. `duration` sets how long should the fault injection controller run for. Use `kubeconfig` argument to specify config file for `kubectl` manually.

All arguments of `inject`:

```shell
-deffile string
    	path to fault definition file
-duration duration
    	Duration of this round of fault simulation
-kubeconfig string
    	(optional) absolute path to the kubeconfig file (default "~/.kube/config")
```

This command is built in `Go`, and to run it you could either run `go build` to first build the executable binary or `go run` to directly build and run the command. 

Example:

```shell
./inject -deffile social-delay.yaml -duration 30m
```

## load

`load` command apply a simulated load that repeats request on one or more `urls`, every time a `delay` has elapsed, for a total `duration`. `users` sets number of concurrent goroutine to simulate multiple users at one time. `body` sets a static text request body for every request to be sent.

```shell
-body string
    	Request body
-delay duration
    	Delay between calls per user (ms) (default 1s)
-duration duration
    	Duration of this load simulation
-url string
    	URLs to perform requests on.
    	Separate each URLs by a whitespace if there're multiple URLs to request on.
    	 (default "http://127.0.0.1")
-users int
    	Number of concurrent users (default 1)
```

This command is built in `Go`, and to run it you could either run `go build` to first build the executable binary or `go run` to directly build and run the command. 

Example:

```shell
./load -delay 100ms -duration 2h -users 5 -url "http://localhost:8080 http://localhost:8081 http://localhost:8082"
```

Apply a load that simulate `5` users repeated request concurrently on `http://localhost:8080`, `http://localhost:8081`, `http://localhost:8082` every `100ms` for `2h`.

## metrics

### Metrics Infrastructure Setup

Currently VECROsim does not support automatically install the `kube-prometheus` monitoring infrastructure. `metrics` folder contains necessary monitoring infrastructure config files and one-key setup/remove shell scripts.

To install `kube-prometheus` monitoring infrastructure to current `Kubernetes` cluster:

```shell
./setup/setup.sh
```

Typical installation would take `3 to 10 minutes`for images to be pulled down.

To remove `kube-prometheus` monitoring infrastructure to current `Kubernetes` cluster:

```shell
./setup/teardown.sh
```

### Export Metrics to Dataset Files

We provide a sample metrics collection script `social_collect.py`  for the `Social` system. To configure the timestamps of collection range, the URL of Prometheus API, etc., modify these lines in the script:

```shell
prometheus_host_url = "http://127.0.0.1:9091/" # The URL of Prometheus API

start_time = parse_datetime("2022-05-22 00:00:00") # The start time of collection
end_time = parse_datetime("2022-05-22 01:00:00") # The end time of collection

step = "1s" # Sample resolution
filepath = "social-delay/jitter_high" # Path to save CSV files
```

To run the script:

```shell
python social_collect.py
```

# Configuration Reference

## System Definition

A microservice `system definition` is a YAML file that define `configuration` of every service and calling `topology` of the system.

Take first 20 lines of the `Social` microservice system as an example:

```yaml
name: social # System name identifier
replicas: 1 # Replica count for every service
namespace: social # Kubernetes namespace this system should be deployed in
services: # Contains a list of services
  - name: follow-user
    type: base # Service image type
    workload: # Define service workload (Optional)
      cpu: 1
      net: 256
    calls: # Contains a list of
           # down-stream services (Optional)
      - user-info
  - name: recommender
    type: base
    workload:
      cpu: 1
      memory: 16
      net: 512
    calls:
      - user-info
      - posts-storage
```

Every `service` entry you define under `services` will be deployed to the `Kubernetes` cluster. 

`type`  of a `service` sets its the service docker image. Available: `base`, `mongodb`, `mysql`, `redis`. The following are details of each type of service type:

| `type`    | `Description`                     | `Supported workload`      |
| --------- | --------------------------------- | ------------------------- |
| `base`    | Generic image of  `logic` service | `cpu`, `io`, `net`, `mem` |
| `mongodb` | `Concrete` service MongoDB        | `read`, `write`           |
| `mysql`   | `Concrete` service MySQL          | `read`, `write`           |
| `redis`   | `Concrete` service Redis          | `read`, `write`           |

`workload`  of a `service` sets its the workload definition. Different service type support different workload types. Please refer to above table for valid workload types. 

`calls`  of a `service` sets its down-stream service list to call when itself get request docker image. Each entry in call list should be a valid `name` defined in the `services` list.

## Fault Definition

A microservice `fault definition` is a YAML file that define `configuration` of expected faults to be injected into the microservice system.

Take first 20 lines of the `base/simple.yaml` fault definition as an example:

```yaml
name: example # Fault definition name identifier
namespace: example # Kubernetes namespace the system is deployed in
faults: # Contains a list of faults
  - name: frontend-downgrade # Fault name
    target: frontend # Fault injection target
    start: 30s # Fault start time
    duration: 45s # Fault duration
    behaviors: # Contains a list of fault behaviors
      net-delay: # Fault behavior and its detailed parameters
        time: 300ms
        jitter: 50ms
  - name: auth-downgrade
    target: auth
    start: 2min
    duration: 45s
    behaviors:
      cpu-stress:
        load: 100
```

`behaviors` are a list of fault `behavior`. The following are details of some representative fault behaviors:

| `type`    | `Description`                     | `Supported parameters`      |
| --------- | --------------------------------- | ------------------------- |
| `net-delay`    | Network ingress delay | `time`, `jitter` |
| `net-loss` | Network loss       | `Percent`          |
| `net-rate`   | Network rate limit          | `Rate`           |
| `io-stress`   | Disk workload I/O stressing          | `Method`           |
| `cpu-stress`   | CPU workload stressing         | `Load`, `Method`           |
