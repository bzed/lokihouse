//go:build oracle && bench

package oracle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// benchS3Key is RustFS's default access and secret key; test/bench/up.sh
// starts RustFS with it.
const benchS3Key = "rustfsadmin"

// newBenchBucket creates a bucket with a fresh name on the S3 server at
// endpoint (host:port, plain HTTP) and returns its name. Every run gets its
// own bucket, so data from an earlier run is never read.
func newBenchBucket(t *testing.T, endpoint string) string {
	t.Helper()
	c, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(benchS3Key, benchS3Key, "")})
	if err != nil {
		t.Fatalf("s3 client for %s: %v", endpoint, err)
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "loki-" + hex.EncodeToString(b)
	if err := c.MakeBucket(context.Background(), name, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatalf("create bucket %s on %s: %v", name, endpoint, err)
	}
	return name
}

// lokiS3Template is Loki's default-query-tuning config with chunks, index and
// delete requests in the S3 bucket.
func lokiS3Template(t *testing.T, endpoint, bucket string) string {
	t.Helper()
	s := lokiDefaultQueryTemplate()
	for _, r := range [][2]string{
		// Loki uses the Thanos object store client by default; its S3
		// settings live under object_store.
		{"    filesystem: {chunks_directory: ${DIR}/chunks}\n", fmt.Sprintf(
			"    object_store:\n      s3: {endpoint: %q, bucket_name: %q, access_key_id: %q, secret_access_key: %q, region: us-east-1, insecure: true, bucket_lookup_type: path}\n",
			endpoint, bucket, benchS3Key, benchS3Key)},
		{"      object_store: filesystem\n", "      object_store: s3\n"},
		{"  delete_request_store: filesystem\n", "  delete_request_store: s3\n"},
	} {
		if !strings.Contains(s, r[0]) {
			t.Fatalf("loki config template has no %q", r[0])
		}
		s = strings.Replace(s, r[0], r[1], 1)
	}
	return s
}

// startLokiS3 starts a writer Loki whose data lives only in a fresh bucket on
// the S3 server at endpoint, and returns its URL and a function to call once
// the data is pushed. That function stops the writer, which flushes its
// chunks and uploads its index as it shuts down, then starts a second Loki
// on the same bucket, whose ingester holds nothing, and returns that one's
// URL. Every query to it reads chunks from S3 (through Loki's embedded chunk
// cache, which Loki enables whenever no other chunk cache is configured), as
// for data older than the ingesters keep.
//
// Loki's querier reads the store through the ingester when both run in one
// process (target all), and query_store_only then returns nothing; a
// separate, empty Loki is the single-binary way to read only the store.
func startLokiS3(t *testing.T, endpoint, extra string) (string, func() string) {
	t.Helper()
	tmpl := lokiS3Template(t, endpoint, newBenchBucket(t, endpoint))
	writer, stop := StartLokiStoppable(t, tmpl, "\ningester: {wal: {flush_on_shutdown: true}}\n"+extra)
	return writer, func() string {
		stop()
		return StartLokiConfig(t, tmpl, extra)
	}
}
