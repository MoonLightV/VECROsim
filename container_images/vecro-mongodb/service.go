package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"math/rand"
	"time"
	"log"
	"go.opentelemetry.io/otel"
)

type BaseService interface {
	Execute(ctx context.Context) (string, error)
}

type baseService struct {
	dbCollection *mongo.Collection

	dbReadOps  int
	dbWriteOps int
}

const itemsCount = 100000
const executionTimeout = 2*time.Second // TODO: make service timeout configurable

func (svc baseService) Execute(ctx context.Context) (string, error) {
	ctx, span := otel.Tracer("vecro-service").Start(ctx, "BaseService.Execute")
	defer span.End()

	//ctx, cancel := context.WithTimeout(ctx, executionTimeout)
	//defer cancel()

	var result struct {
		Value float64
	}

	lastResult := ""

	// Perform read operations
	for i := 0; i < svc.dbReadOps; i++ {
		id := rand.Intn(itemsCount)
		filter := bson.D{{"id", id}}
		log.Printf("Info db read id: %d\n", id)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := svc.dbCollection.FindOne(ctx, filter).Decode(&result)
		log.Printf("Info db read: %d\n", id)
		if err == mongo.ErrNoDocuments {
			return "id not found", nil
		} else if err != nil {
			return "failed to find in the collection", nil
		}

		lastResult = fmt.Sprintf("%f", result.Value)
		log.Printf("Info db read result: %s\n", lastResult)
	}

	// Perform write operations
	for i := 0; i < svc.dbWriteOps; i++ {
		document := bson.D{{"app", "vecro-sim"}, {"rand_value", rand.Float64()}}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := svc.dbCollection.InsertOne(ctx, document)
		if err != nil {
			return "failed to insert into the collection", nil
		}

		lastResult = fmt.Sprintf("%v", result.InsertedID)
		log.Printf("Info db insert result: %s\n", lastResult)
	}

	return lastResult, nil
}

type ServiceMiddleware func(BaseService) BaseService
