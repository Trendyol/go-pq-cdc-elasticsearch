package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type InfraStructure struct {
	PostgresHost     string
	PostgresPort     string
	ElasticsearchURL string
}

var (
	PostgresTestImage = "POSTGRES_TEST_IMAGE"
	defaultVersion    = "16.2"
)

func SetupInfra() (*InfraStructure, func()) {
	ctx := context.Background()

	// Get PostgreSQL version from environment or use default
	postgresVersion := os.Getenv(PostgresTestImage)
	if postgresVersion == "" {
		postgresVersion = defaultVersion
	}

	// PostgreSQL container
	postgresPort := "5432/tcp"
	postgresReq := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("postgres:%s", postgresVersion),
		ExposedPorts: []string{postgresPort},
		Env: map[string]string{
			"POSTGRES_USER":             "cdc_user",
			"POSTGRES_PASSWORD":         "cdc_pass",
			"POSTGRES_DB":               "cdc_db",
			"POSTGRES_HOST_AUTH_METHOD": "trust",
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
			"-c", "max_wal_senders=10",
			"-c", "max_replication_slots=10",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}

	postgresContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: postgresReq,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("failed to start postgres container: %v", err)
	}

	postgresHost, err := postgresContainer.Host(ctx)
	if err != nil {
		log.Fatalf("failed to get postgres host: %v", err)
	}

	postgresMappedPort, err := postgresContainer.MappedPort(ctx, nat.Port(postgresPort))
	if err != nil {
		log.Fatalf("failed to get postgres mapped port: %v", err)
	}

	// Elasticsearch container
	elasticsearchReq := testcontainers.ContainerRequest{
		Image:        "docker.elastic.co/elasticsearch/elasticsearch:7.17.10",
		ExposedPorts: []string{"9200:9200/tcp"},
		Env: map[string]string{
			"discovery.type":         "single-node",
			"xpack.security.enabled": "false",
			"ES_JAVA_OPTS":           "-Xms512m -Xmx512m",
		},
		WaitingFor: wait.ForHTTP("/_cluster/health?wait_for_status=yellow&timeout=60s").
			WithStartupTimeout(180 * time.Second).
			WithPollInterval(2 * time.Second),
	}

	elasticsearchContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: elasticsearchReq,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("failed to start elasticsearch container: %v", err)
	}

	elasticsearchMappedPort, err := elasticsearchContainer.MappedPort(ctx, "9200")
	if err != nil {
		log.Fatalf("failed to get elasticsearch port: %v", err)
	}

	infra := &InfraStructure{
		PostgresHost:     postgresHost,
		PostgresPort:     postgresMappedPort.Port(),
		ElasticsearchURL: fmt.Sprintf("http://localhost:%s", elasticsearchMappedPort.Port()),
	}

	log.Printf("PostgreSQL started at %s:%s", infra.PostgresHost, infra.PostgresPort)
	log.Printf("Elasticsearch started at %s", infra.ElasticsearchURL)

	cleanup := func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate postgres container: %v", err)
		}
		if err := elasticsearchContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate elasticsearch container: %v", err)
		}
	}

	return infra, cleanup
}
