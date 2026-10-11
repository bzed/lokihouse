package api

import (
	"encoding/json"
	"net/http"

	"github.com/bzed/lokihouse/internal/build"
)

func buildInfo(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(build.Info()) // a failed write means the client went away
}
