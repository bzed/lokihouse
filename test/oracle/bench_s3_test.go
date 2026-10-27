//go:build oracle && bench

package oracle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// benchS3Key is RustFS's default access and secret key; test/bench/up.sh
// starts RustFS with it.
const benchS3Key = "rustfsadmin"

// benchS3 is the S3 store the object-storage scenario's Loki keeps its data
// in: a fresh bucket on RustFS, or a fresh prefix in an existing bucket of a
// remote store (benchRemoteS3).
type benchS3 struct {
	endpoint, region, bucket, prefix, key, secret string
	insecure                                      bool
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s benchS3) client(t *testing.T) *minio.Client {
	t.Helper()
	c, err := minio.New(s.endpoint, &minio.Options{Creds: credentials.NewStaticV4(s.key, s.secret, ""), Secure: !s.insecure})
	if err != nil {
		t.Fatalf("s3 client for %s: %v", s.endpoint, err)
	}
	return c
}

// newBenchBucket creates a bucket with a fresh name on the S3 server at
// endpoint (host:port, plain HTTP) and returns it. Every run gets its own
// bucket, so data from an earlier run is never read.
func newBenchBucket(t *testing.T, endpoint string) benchS3 {
	t.Helper()
	s := benchS3{endpoint: endpoint, region: "us-east-1", bucket: "loki-" + randHex(6), key: benchS3Key, secret: benchS3Key, insecure: true}
	if err := s.client(t).MakeBucket(context.Background(), s.bucket, minio.MakeBucketOptions{Region: s.region}); err != nil {
		t.Fatalf("create bucket %s on %s: %v", s.bucket, endpoint, err)
	}
	return s
}

// benchRemoteS3 returns the remote store named by S3_ENDPOINT (an https URL),
// S3_REGION, S3_BUCKET, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (the
// variables of .secrets/s3.env), with a fresh prefix of its own in the
// bucket. The objects under the prefix are removed when the test ends, so a
// shared bucket stays clean.
func benchRemoteS3(t *testing.T) benchS3 {
	t.Helper()
	s := benchS3{
		endpoint: strings.TrimPrefix(os.Getenv("S3_ENDPOINT"), "https://"),
		region:   os.Getenv("S3_REGION"),
		bucket:   os.Getenv("S3_BUCKET"),
		key:      os.Getenv("AWS_ACCESS_KEY_ID"),
		secret:   os.Getenv("AWS_SECRET_ACCESS_KEY"),
		prefix:   "lokihouse-bench-" + randHex(6),
	}
	if s.endpoint == "" || s.region == "" || s.bucket == "" || s.key == "" || s.secret == "" {
		t.Fatal("LOKIHOUSE_BENCH_S3_REMOTE needs S3_ENDPOINT, S3_REGION, S3_BUCKET, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (make bench-s3 loads .secrets/s3.env)")
	}
	t.Cleanup(func() {
		c := s.client(t)
		ctx := context.Background()
		// One DELETE per object: the store rejects the bulk delete request.
		for o := range c.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.prefix, Recursive: true}) {
			if o.Err != nil {
				t.Logf("list %s: %v", s.prefix, o.Err)
				break
			}
			if err := c.RemoveObject(ctx, s.bucket, o.Key, minio.RemoveObjectOptions{}); err != nil {
				t.Logf("remove %s: %v", o.Key, err)
			}
		}
	})
	return s
}

// lokiS3Template is Loki's default-query-tuning config with chunks, index and
// delete requests in the S3 store s.
func lokiS3Template(t *testing.T, s benchS3) string {
	t.Helper()
	tmpl := lokiDefaultQueryTemplate()
	prefix := ""
	if s.prefix != "" {
		prefix = fmt.Sprintf("      storage_prefix: %q\n", s.prefix)
	}
	for _, r := range [][2]string{
		// Loki uses the Thanos object store client by default; its S3
		// settings live under object_store.
		{"    filesystem: {chunks_directory: ${DIR}/chunks}\n", fmt.Sprintf(
			"    object_store:\n%s      s3: {endpoint: %q, bucket_name: %q, access_key_id: %q, secret_access_key: %q, region: %q, insecure: %t, bucket_lookup_type: path}\n",
			prefix, s.endpoint, s.bucket, s.key, s.secret, s.region, s.insecure)},
		{"      object_store: filesystem\n", "      object_store: s3\n"},
		{"  delete_request_store: filesystem\n", "  delete_request_store: s3\n"},
	} {
		if !strings.Contains(tmpl, r[0]) {
			t.Fatalf("loki config template has no %q", r[0])
		}
		tmpl = strings.Replace(tmpl, r[0], r[1], 1)
	}
	return tmpl
}

// startLokiS3 starts a writer Loki whose data lives only in a fresh bucket on
// the S3 store s, and returns its URL and a function to call once
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
func startLokiS3(t *testing.T, s benchS3, extra string) (string, func() string) {
	t.Helper()
	tmpl := lokiS3Template(t, s)
	writer, stop := StartLokiStoppable(t, tmpl, "\ningester: {wal: {flush_on_shutdown: true}}\n"+extra)
	return writer, func() string {
		stop()
		return StartLokiConfig(t, tmpl, extra)
	}
}
