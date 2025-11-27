package main

import (
	"context"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	slog "log"

	"github.com/go-kit/kit/endpoint"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/go-kit/kit/log"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	httptransport "github.com/go-kit/kit/transport/http"

	"go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/jaeger"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/propagation"
)

type contextKey string

const contextKeyRequest contextKey = "http-request"


func initTracer() func(context.Context) error {
	serviceName := os.Getenv("VECRO_NAME")
	if serviceName == "" {
		serviceName = "default-service"
	}

	exporter, err := jaeger.New(jaeger.WithCollectorEndpoint(
		jaeger.WithEndpoint("http://jaeger-collector:14268/api/traces"),
	))
	if err != nil {
		slog.Printf("failed to create Jaeger exporter: %v", err)
	} else {
		slog.Println("success to build jaeger")
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName), // 动态命名
		)),
	)

	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(tp)

	return tp.Shutdown
}

func tracingMiddleware(tracerName, spanName string) endpoint.Middleware {
	tracer := otel.Tracer(tracerName)

	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request interface{}) (interface{}, error) {
			if r, ok := ctx.Value(contextKeyRequest).(*http.Request); ok {
				ctx = propagator.Extract(ctx, propagation.HeaderCarrier(r.Header))
			}

			ctx, span := tracer.Start(ctx, spanName)
			defer span.End()

			return next(ctx, request)
		}
	}
}

func main() {
	// 初始化 Tracer 并注册 Shutdown
	shutdown := initTracer()
	defer shutdown(context.Background())
	// -------------------
	// Declare constants
	// -------------------
	const (
		nameEnvKey          = "VECRO_NAME"
		subsystemEnvKey     = "VECRO_SUBSYSTEM"
		listenAddressEnvKey = "VECRO_LISTEN_ADDRESS"
		callEnvKey          = "VECRO_CALLS"
		callSeparator       = " "
	)

	const (
		workloadCPUEnvKey         = "VECRO_WORKLOAD_CPU"
		workloadIOEnvKey          = "VECRO_WORKLOAD_IO"
		workloadDelayTimeEnvKey   = "VECRO_WORKLOAD_DELAY_TIME"
		workloadDelayJitterEnvKey = "VECRO_WORKLOAD_DELAY_JITTER"
		workloadNetEnvKey         = "VECRO_WORKLOAD_NET"
		workloadMemoryEnvKey      = "VECRO_WORKLOAD_MEMORY"
	)

	// -------------------
	// Init logging
	// -------------------
	var logger log.Logger
	logger = log.NewLogfmtLogger(os.Stderr)
	logger = log.With(logger, "caller", log.DefaultCaller)

	// -------------------
	// Parse Environment variables
	// -------------------
	var (
		delayTime   int
		delayJitter int
		cpuLoad     int
		ioLoad      int
		netLoad     int
		memLoad     int
	)
	delayTime, _ = getEnvInt(workloadDelayTimeEnvKey, 0)
	delayJitter, _ = getEnvInt(workloadDelayJitterEnvKey, delayTime/10)
	cpuLoad, _ = getEnvInt(workloadCPUEnvKey, 0)
	ioLoad, _ = getEnvInt(workloadIOEnvKey, 0)
	netLoad, _ = getEnvInt(workloadNetEnvKey, 0)
	memLoad, _ = getEnvInt(workloadMemoryEnvKey, 0)

	slog.Println("Info Delay time:", delayTime)
	slog.Println("Info Delay jitter:", delayJitter)
	slog.Println("Info CPU load:", cpuLoad)
	slog.Println("Info IO load:", ioLoad)
	slog.Println("Info Net load", netLoad)

	listenAddress, _ := getEnvString(listenAddressEnvKey, ":8080")
	slog.Println("Info listen_address:", listenAddress)

	subsystem, _ := getEnvString(subsystemEnvKey, "subsystem")
	name, _ := getEnvString(nameEnvKey, "name")
	slog.Println("Info Name:", name, "Subsystem:", subsystem)

	// -------------------
	// Init Prometheus counter & histogram
	// -------------------
	requestCount := kitprometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Namespace: "vecro_base",
		Subsystem: subsystem,
		Name:      "request_count",
		Help:      "Number of requests received.",
		ConstLabels: map[string]string{
			"vecrosim_service_name": name,
		},
	}, nil)
	latencyCounter := kitprometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Namespace: "vecro_base",
		Subsystem: subsystem,
		Name:      "latency_counter",
		Help:      "Processing time taken of requests in seconds, as counter.",
		ConstLabels: map[string]string{
			"vecrosim_service_name": name,
		},
	}, nil)
	latencyHistogram := kitprometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "vecro_base",
		Subsystem: subsystem,
		Name:      "latency_histogram",
		Help:      "Processing time taken of requests in seconds, as histogram.",
		// TODO: determine appropriate buckets
		Buckets: []float64{.0002, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 15, 25},
		ConstLabels: map[string]string{
			"vecrosim_service_name": name,
		},
	}, nil)
	throughput := kitprometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Namespace: "vecro_base",
		Subsystem: subsystem,
		Name:      "throughput",
		Help:      "Size of data transmitted in bytes.",
		ConstLabels: map[string]string{
			"vecrosim_service_name": name,
		},
	}, nil)

	// -------------------
	// Init call endpoints
	// -------------------

	// Create call endpoint list from the environment variable
	var calls []endpoint.Endpoint
	callList, exists := getEnvString(callEnvKey, "")
	tracerName := os.Getenv("VECRO_NAME")
	if exists {
		slog.Println("Info Calls", callList)

		for _, callStr := range strings.Split(callList, callSeparator) {
			callURL, err := url.Parse(callStr)
			if err != nil {
				panic(err)
			}
			callEndpoint := httptransport.NewClient(
				"GET",
				callURL,
				encodeRequest,
				decodeBaseResponse,
			).Endpoint()
		
			// 包裹 trace 中间件
			tracedEndpoint := tracingMiddleware(tracerName, "downstream-call")(callEndpoint)
		
			calls = append(calls, tracedEndpoint)
		}
	} else {
		slog.Println("Info Calls", "[empty call list]")
	}

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Simulate memory allocation
	memBlock := allocMemory(memLoad)
	_ = memBlock

	// -------------------
	// Create & run service
	// -------------------
	var svc BaseService
	svc = baseService{
		calls:       calls,
		delayTime:   delayTime,
		delayJitter: delayJitter,
		cpuLoad:     cpuLoad,
		ioLoad:      ioLoad,
		netLoad:     netLoad,
	}
	svc = loggingMiddleware(logger)(svc)
	svc = instrumentingMiddleware(requestCount, latencyCounter, latencyHistogram, logger)(svc)

	baseEndpoint := makeBaseEndPoint(svc)
	baseEndpoint = tracingMiddleware("vecro-service", "BaseRequest")(baseEndpoint)

	baseHandler := httptransport.NewServer(
		//makeBaseEndPoint(svc),
		baseEndpoint,
		decodeBaseRequest,
		encodeResponse,
		httptransport.ServerBefore(func(ctx context.Context, r *http.Request) context.Context {
			return context.WithValue(ctx, contextKeyRequest, r)
		}),
		// Request throughput instrumentation
		httptransport.ServerFinalizer(func(ctx context.Context, code int, r *http.Request) {
			responseSize := ctx.Value(httptransport.ContextKeyResponseSize).(int64)
			slog.Println("Info reponse_size:", responseSize)
			throughput.Add(float64(responseSize))
		}),
	)

	http.Handle("/", baseHandler)
	http.Handle("/metrics", promhttp.Handler())
	slog.Println("Info msg:", "HTTP", "addr:", listenAddress)
	logger.Log("err", http.ListenAndServe(listenAddress, nil))
}
