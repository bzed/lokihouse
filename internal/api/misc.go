package api

import (
	"encoding/json"
	"net/http"
)

// Version is set at build time with -ldflags "-X github.com/bzed/lokihouse/internal/api.Version=...".
var Version = "dev"

func buildInfo(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"version": Version, "revision": "", "branch": "", "buildUser": "", "buildDate": "", "goVersion": ""})
}
