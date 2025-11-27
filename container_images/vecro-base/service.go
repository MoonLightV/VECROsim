package main

import (
	"context"
	"github.com/go-kit/kit/endpoint"
	"strings"
	"time"
	"log"
	"go.opentelemetry.io/otel"
)

type BaseService interface {
	Execute(ctx context.Context) (string, error)
}

type baseService struct {
	calls         []endpoint.Endpoint // Downstream endpoints to be called on
	isSynchronous bool                // Whether to call services synchronously or asynchronously.  TODO: NOT IMPLEMENTED YET!

	delayTime   int
	delayJitter int
	cpuLoad     int
	ioLoad      int
	netLoad     int
}

const executionTimeout = 2*time.Second // TODO: make service timeout configurable

func (svc baseService) Execute(ctx context.Context) (string, error) {
	ctx, span := otel.Tracer("vecro-service").Start(ctx, "BaseService.Execute")
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, executionTimeout)
	defer cancel()

	log.Println("Info Starting service execution with timeout:", executionTimeout)
	log.Printf("Info Simulating stress with delayTime=%d, cpuLoad=%d", svc.delayTime, svc.cpuLoad)
	stress(svc.delayTime, svc.delayJitter, svc.cpuLoad, svc.ioLoad)

	for _, ep := range svc.calls {
		_, err := ep(ctx, nil)
		if err != nil {
			log.Printf("Error Downstream call %s failed: %v", ep, err)
			return "", err
		}
	}

	var payload string
	if svc.netLoad > 0 {
		payload = strings.Repeat("0", svc.netLoad/2)
		log.Printf("Info Generated payload of size: %d", len(payload))
	}

	return payload, nil
}

type ServiceMiddleware func(BaseService) BaseService
