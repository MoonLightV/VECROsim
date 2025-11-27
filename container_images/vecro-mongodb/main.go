package main

import (
	"context"
	"math/rand"
	"net/http"
	"os"
	"time"
	slog "log"

	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/go-kit/kit/log"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	httptransport "github.com/go-kit/kit/transport/http"

	"go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/jaeger"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/propagation"
	"github.com/go-kit/kit/endpoint"
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
	shutdown := initTracer()
	defer shutdown(context.Background())
	// -------------------
	// Declare constants
	// -------------------
	const (
		nameEnvKey          = "VECRO_NAME"
		subsystemEnvKey     = "VECRO_SUBSYSTEM"
		listenAddressEnvKey = "VECRO_LISTEN_ADDRESS"
		dbReadOpsEnvKey     = "VECRO_DB_READ_OPS"
		dbWriteOpsEnvKey    = "VECRO_DB_WRITE_OPS"
		dbUserEnvKey        = "VECRO_DB_USER"
		dbPasswordEnvKey    = "VECRO_DB_PASSWORD"
		dbCollectionEnvKey  = "VECRO_DB_COLLECTION"
	)

	const databaseName = "data"
	const collectionName = "items"

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
		dbReadOps    int
		dbWriteOps   int
		dbUser       string
		dbPassword   string
		dbCollection string
	)
	dbReadOps, _ = getEnvInt(dbReadOpsEnvKey, 0)
	dbWriteOps, _ = getEnvInt(dbWriteOpsEnvKey, 0)
	dbUser, _ = getEnvString(dbUserEnvKey, "")
	dbPassword, _ = getEnvString(dbPasswordEnvKey, "")
	dbCollection, _ = getEnvString(dbCollectionEnvKey, "")

	slog.Println("Info db read ops:", dbReadOps)
	slog.Println("Info db write ops:", dbWriteOps)
	slog.Println("Info db user:", dbUser)
	slog.Println("Info db password:", dbPassword)
	slog.Println("Info db collection:", dbCollection)

	listenAddress, _ := getEnvString(listenAddressEnvKey, ":8080")
	slog.Println("Info listen_address:", listenAddress)

	subsystem, _ := getEnvString(subsystemEnvKey, "subsystem")
	name, _ := getEnvString(nameEnvKey, "name")
	slog.Println("Info name:", name, "subsystem:", subsystem)

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
	// Init database connection
	// -------------------

	// Connect to database and locate the collection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	credential := options.Credential{
		Username: "root",
		Password: "password",
	}
	clientOpts := options.Client().
		ApplyURI("mongodb://localhost").
		SetAuth(credential)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		panic(err)
	}
	err = client.Ping(ctx, nil)
	if err != nil {
		panic(err)
	}

	collection := client.Database(databaseName).Collection(collectionName)

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// -------------------
	// Create & run service
	// -------------------
	var svc BaseService
	svc = baseService{
		dbCollection: collection,
		dbReadOps:    dbReadOps,
		dbWriteOps:   dbWriteOps,
	}
	svc = loggingMiddleware(logger)(svc)
	svc = instrumentingMiddleware(requestCount, latencyCounter, latencyHistogram, logger)(svc)

	baseEndpoint := makeBaseEndPoint(svc)
	baseEndpoint = tracingMiddleware("vecro-service", "BaseRequest")(baseEndpoint)

	baseHandler := httptransport.NewServer(
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
