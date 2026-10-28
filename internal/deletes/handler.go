package deletes

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/compactor/deletion"
	"github.com/grafana/loki/v3/pkg/compactor/deletion/deletionproto"
	"github.com/grafana/loki/v3/pkg/compactor/deletionmode"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/util"
	"github.com/prometheus/common/model"
)

// Copied from grafana/loki@39c256e5 pkg/compactor/deletion/request_handler.go and
// tenant_request_handler.go (handler methods, interval, query, startTime, endTime,
// parseTime, timeFromInt, TenantMiddleware, validDeletionLimit, deleteModeFromLimits)
// and util.go (parseDeletionQuery); modifications:
//   - the store calls go to *Store (Add/Get/Cancel instead of AddDeleteRequest/
//     GetDeleteRequest/RemoveDeleteRequest);
//   - max_interval is validated exactly as Loki does, but requests are stored
//     unsharded: the shards only drive Loki's compactor, which the listing merges
//     back, and lokihouse has no compactor to feed;
//   - the metrics are dropped;
//   - the gate is a Gate method wrapping the next handler, since lokihouse's
//     tenant wrapper is applied by the API router;
//   - the gate reads Limits.DeletionMode(tenant).
const (
	deletionNotAvailableMsg         = "deletion is not available for this tenant"
	forQuerytimeFilteringQueryParam = "for_querytime_filtering"
)

var errInvalidQuery = errors.New("invalid query expression")

// Handler serves the delete API endpoints.
type Handler struct {
	Store        *Store
	Limits       deletionLimits
	MaxInterval  time.Duration
	CancelPeriod time.Duration
}

type deletionLimits interface {
	DeletionMode(userID string) string
}

// Gate wraps h with the tenant + deletion-mode check, exactly as Loki's
// TenantMiddleware does.
func (h *Handler) Gate(next http.Handler) http.Handler {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			userID, err := tenant.TenantID(ctx)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			hasDelete, err := validDeletionLimit(h.Limits, userID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if !hasDelete {
				http.Error(w, deletionNotAvailableMsg, http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	})(next)
}

// Add handles addition of a new delete request.
func (h *Handler) Add(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	params := r.URL.Query()
	query, parsedExpr, err := query(params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	startTime, err := startTime(params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	endTime, err := endTime(params, startTime)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var shardByInterval time.Duration
	// shard delete requests only when there are line filters
	if parsedExpr.HasFilter() {
		shardByInterval, err = h.interval(params, startTime, endTime)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	requestID, err := h.Store.Add(ctx, userID, query, startTime, endTime)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_ = shardByInterval // drives Loki's compactor shards; lokihouse stores unsharded
	w.Header().Set("X-Delete-Request-ID", requestID)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) interval(params url.Values, startTime, endTime model.Time) (time.Duration, error) {
	qr := params.Get("max_interval")
	if qr == "" {
		return h.MaxInterval, nil
	}

	interval, err := time.ParseDuration(qr)
	if err != nil || interval < time.Second {
		return 0, errors.New("invalid max_interval: valid time units are 's', 'm', 'h'")
	}

	if interval > h.MaxInterval && h.MaxInterval != 0 {
		return 0, fmt.Errorf("max_interval can't be greater than %s", h.MaxInterval.String())
	}

	if interval > endTime.Sub(startTime) {
		return 0, fmt.Errorf("max_interval can't be greater than the interval to be deleted (%s)", endTime.Sub(startTime))
	}

	return interval, nil
}

// List handles get all delete requests.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	forQuerytimeFiltering := r.URL.Query().Get(forQuerytimeFilteringQueryParam) == "true"

	// optional time range overlap filtering
	var timeRange *deletion.TimeRange
	if sp, ep := r.URL.Query().Get("start"), r.URL.Query().Get("end"); sp != "" && ep != "" {
		st, err := parseTime(sp)
		if err != nil {
			http.Error(w, "invalid start time: require unix seconds or RFC3339 format", http.StatusBadRequest)
			return
		}

		et, err := parseTime(ep)
		if err != nil {
			http.Error(w, "invalid end time: require unix seconds or RFC3339 format", http.StatusBadRequest)
			return
		}

		timeRange = &deletion.TimeRange{
			Start: model.Time(st),
			End:   model.Time(et),
		}
	}

	deleteRequests, err := h.Store.GetAllDeleteRequestsForUser(ctx, userID, forQuerytimeFiltering, timeRange)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// We have to retain UserID in the proto encoding of deletion manifests.
	// However, we do not want to return it to the users since it is not relevant.
	for i := range deleteRequests {
		deleteRequests[i].UserID = ""
	}

	sort.Slice(deleteRequests, func(i, j int) bool {
		return deleteRequests[i].CreatedAt < deleteRequests[j].CreatedAt
	})

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(deleteRequests); err != nil {
		http.Error(w, fmt.Sprintf("Error marshalling response: %v", err), http.StatusInternalServerError)
	}
}

// Cancel handles delete request cancellation.
func (h *Handler) Cancel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	params := r.URL.Query()
	requestID := params.Get("request_id")
	deleteRequest, err := h.Store.Get(ctx, userID, requestID)
	if err != nil {
		if errors.Is(err, deletion.ErrDeleteRequestNotFound) {
			http.Error(w, "could not find delete request with given id", http.StatusNotFound)
			return
		}

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if deleteRequest.Status == deletionproto.StatusProcessed {
		http.Error(w, "deletion of request which is in process or already processed is not allowed", http.StatusBadRequest)
		return
	}

	if (deleteRequest.Status != deletionproto.StatusReceived || deleteRequest.CreatedAt.Add(h.CancelPeriod).Before(model.Now())) && params.Get("force") != "true" {
		http.Error(w, fmt.Sprintf("Cancellation of partially completed delete request or delete request past the deadline of %s since its creation is not allowed. To force, use the ?force query parameter", h.CancelPeriod.String()), http.StatusBadRequest)
		return
	}

	if err := h.Store.Cancel(ctx, userID, requestID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func query(params url.Values) (string, syntax.LogSelectorExpr, error) {
	query := params.Get("query")
	if len(query) == 0 {
		return "", nil, errors.New("query not set")
	}

	parsedExpr, err := parseDeletionQuery(query)
	if err != nil {
		return "", nil, err
	}

	return query, parsedExpr, nil
}

func startTime(params url.Values) (model.Time, error) {
	startParam := params.Get("start")
	if startParam == "" {
		return 0, errors.New("start time not set")
	}

	st, err := parseTime(startParam)
	if err != nil {
		return 0, errors.New("invalid start time: require unix seconds or RFC3339 format")
	}

	return model.Time(st), nil
}

func endTime(params url.Values, startTime model.Time) (model.Time, error) {
	endParam := params.Get("end")
	endTime, err := parseTime(endParam)
	if err != nil {
		return 0, errors.New("invalid end time: require unix seconds or RFC3339 format")
	}

	if endTime > int64(model.Now()) {
		return 0, errors.New("deletes in the future are not allowed")
	}

	if int64(startTime) >= endTime {
		return 0, errors.New("start time can't be greater than or equal to end time")
	}

	return model.Time(endTime), nil
}

func parseTime(in string) (int64, error) {
	if in == "" {
		return int64(model.Now()), nil
	}

	t, err := time.Parse(time.RFC3339, in)
	if err != nil {
		return timeFromInt(in)
	}

	return t.UnixMilli(), nil
}

func timeFromInt(in string) (int64, error) {
	if len(in) != 10 {
		return 0, errors.New("not unix seconds")
	}

	return util.ParseTime(in)
}

// parseDeletionQuery checks if the given logQL is valid for deletions.
func parseDeletionQuery(query string) (syntax.LogSelectorExpr, error) {
	logSelectorExpr, err := syntax.ParseLogSelector(query, true)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidQuery, err)
	}

	// Some stages are only checked when the pipeline gets built, for instance line
	// filter regexes and ip() patterns. Build it here and throw it away so we reject
	// the request now, instead of failing every time we try to process it later.
	if _, err := logSelectorExpr.Pipeline(); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidQuery, err)
	}

	return logSelectorExpr, nil
}

func validDeletionLimit(l deletionLimits, userID string) (bool, error) {
	mode, err := deleteModeFromLimits(l, userID)
	if err != nil {
		return false, err
	}

	return mode.DeleteEnabled(), nil
}

func deleteModeFromLimits(l deletionLimits, userID string) (deletionmode.Mode, error) {
	mode := l.DeletionMode(userID)
	return deletionmode.ParseMode(mode)
}
