package testutil

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func SetupRedis(t *testing.T) *redis.Client {
	t.Helper()

	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       1,
		Protocol: 2,
	})

	err := client.Ping(ctx).Err()
	if err != nil {
		t.Fatalf("failed to connect to Redis: %v", err)
	}

	err = client.FlushDB(ctx).Err()
	if err != nil {
		t.Fatalf("failed to clear Redis before test: %v", err)
	}

	t.Cleanup(func() {
		client.FlushDB(ctx)
		client.Close()
	})

	return client
}