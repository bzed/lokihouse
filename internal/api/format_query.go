package api

import (
	"encoding/json"
	"net/http"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

// FormatQueryHandler returns a handler that formats a LogQL expression,
// copied from Loki (see attribution below). Unlike the other routes it is
// registered without the tenant wrapper: Loki serves it without auth.

// Copyright (c) 2015-2026 Grafana Labs.
// Copied from grafana/loki@fbc3f077 pkg/loki/format_query_handler.go;
// modifications: moved into the api package.

// FormatQueryResponse is the JSON body of /loki/api/v1/format_query.
type FormatQueryResponse struct {
	Status string `json:"status"`
	Data   string `json:"data,omitempty"`
	Err    string `json:"error,omitempty"`
}

// FormatQueryHandler formats the "query" form value with syntax.Prettify.
func FormatQueryHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			statusCode = http.StatusOK
			status     = "success"
			formatted  string
			errStr     string
		)

		expr, err := syntax.ParseExpr(r.FormValue("query"))
		if err != nil {
			statusCode = http.StatusBadRequest
			status = "invalid-query"
			errStr = err.Error()
		}

		if err == nil {
			formatted = syntax.Prettify(expr)
		}

		resp := FormatQueryResponse{
			Status: status,
			Data:   formatted,
			Err:    errStr,
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(statusCode)

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
