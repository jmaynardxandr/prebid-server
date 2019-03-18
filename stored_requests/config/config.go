package config

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
	"github.com/prebid/prebid-server/config"
	"github.com/prebid/prebid-server/stored_requests"
	"github.com/prebid/prebid-server/stored_requests/backends/db_fetcher"
	"github.com/prebid/prebid-server/stored_requests/backends/empty_fetcher"
	"github.com/prebid/prebid-server/stored_requests/backends/file_fetcher"
	"github.com/prebid/prebid-server/stored_requests/backends/http_fetcher"
	"github.com/prebid/prebid-server/stored_requests/caches/memory"
	"github.com/prebid/prebid-server/stored_requests/caches/nil_cache"
	"github.com/prebid/prebid-server/stored_requests/events"
	apiEvents "github.com/prebid/prebid-server/stored_requests/events/api"
	httpEvents "github.com/prebid/prebid-server/stored_requests/events/http"
	postgresEvents "github.com/prebid/prebid-server/stored_requests/events/postgres"
)

type databaseUsageCount struct {
	conn string
	db   *sql.DB
	refs int
}

var dbUsageByConn = map[string]*databaseUsageCount{}
var dbUsageByDb = map[*sql.DB]*databaseUsageCount{}

// NewStoredRequests returns three things:
//
// 1. A Fetcher which can be used to get Stored Requests
// 2. A DB connection, if one was created. This may be nil.
// 3. A function which should be called on shutdown for graceful cleanups.
//
// If any errors occur, the program will exit with an error message.
// It probably means you have a bad config or networking issue.
//
// As a side-effect, it will add some endpoints to the router if the config calls for it.
// In the future we should look for ways to simplify this so that it's not doing two things.
func NewStoredRequests(cfg *config.StoredRequests, client *http.Client, router *httprouter.Router) (fetcher stored_requests.AllFetcher, db *sql.DB, shutdown func()) {
	// Create DB (or reuse existing DB if same DB used for multiple stored requests)
	if cfg.Postgres.ConnectionInfo.Database != "" {
		conn := cfg.Postgres.ConnectionInfo.ConnString()

		if usage, ok := dbUsageByConn[conn]; ok {
			glog.Infof("Reusing connection to Postgres for Stored Requests. DB=%s, host=%s, port=%d, user=%s",
				cfg.Postgres.ConnectionInfo.Database,
				cfg.Postgres.ConnectionInfo.Host,
				cfg.Postgres.ConnectionInfo.Port,
				cfg.Postgres.ConnectionInfo.Username)
			db = usage.db
			usage.refs++
		} else {
			glog.Infof("Connecting to Postgres for Stored Requests. DB=%s, host=%s, port=%d, user=%s",
				cfg.Postgres.ConnectionInfo.Database,
				cfg.Postgres.ConnectionInfo.Host,
				cfg.Postgres.ConnectionInfo.Port,
				cfg.Postgres.ConnectionInfo.Username)
			db = newPostgresDB(cfg.Postgres.ConnectionInfo)
			usage = &databaseUsageCount{conn: conn, db: db, refs: 1}
			dbUsageByConn[conn] = usage
			dbUsageByDb[db] = usage
		}
	}

	eventProducers := newEventProducers(cfg, client, db, router)
	cache := newCache(cfg)
	fetcher = newFetcher(cfg, client, db)
	fetcher = stored_requests.WithCache(fetcher, cache)

	shutdown1 := addListeners(cache, eventProducers)
	shutdown = func() {
		shutdown1()
		if db != nil {
			if usage, ok := dbUsageByDb[db]; ok {
				// Decrease reference count, cleanup db when it reaches zero
				usage.refs--
				if usage.refs == 0 {
					delete(dbUsageByConn, usage.conn)
					delete(dbUsageByDb, db)
					if err := db.Close(); err != nil {
						glog.Errorf("Error closing DB connection: %v", err)
					}
				}
			}
		}
	}

	return
}

// CreateStoredRequests returns four things:
//
// 1. A Fetcher which can be used to get Stored Requests for /openrtb2/auction
// 2. A Fetcher which can be used to get Stored Requests for /openrtb2/amp
// 3. A Fetcher which can be used to get Category Mapping data
// 4. A function which should be called on shutdown for graceful cleanups.
//
// If any errors occur, the program will exit with an error message.
// It probably means you have a bad config or networking issue.
//
// As a side-effect, it will add some endpoints to the router if the config calls for it.
// In the future we should look for ways to simplify this so that it's not doing two things.
func CreateStoredRequests(cfg *config.Configuration, client *http.Client, router *httprouter.Router) (fetcher stored_requests.Fetcher, ampFetcher stored_requests.Fetcher, db []*sql.DB, shutdown func(), categoriesFetcher stored_requests.CategoryFetcher) {
	resolvedStoredRequestsConfig(cfg)

	fetcher1, db1, shutdown1 := NewStoredRequests(&cfg.StoredRequests, client, router)
	fetcher2, db2, shutdown2 := NewStoredRequests(&cfg.AmpStoredRequests, client, router)
	fetcher3, db3, shutdown3 := NewStoredRequests(&cfg.CategoryMapping, client, router)

	fetcher = fetcher1.(stored_requests.Fetcher)
	ampFetcher = fetcher2.(stored_requests.Fetcher)
	categoriesFetcher = fetcher3.(stored_requests.CategoryFetcher)

	db = make([]*sql.DB, 0, 3)
	if db1 != nil {
		db = append(db, db1)
	}
	if db2 != nil && db2 != db1 {
		db = append(db, db2)
	}
	if db3 != nil && db3 != db2 && db3 != db1 {
		db = append(db, db3)
	}

	shutdown = func() {
		shutdown1()
		shutdown2()
		shutdown3()
	}

	return
}

func resolvedStoredRequestsConfig(cfg *config.Configuration) {
	// For backwards compatibility:
	// If AmpStoredRequests is not filled in but Amp fields of StoredRequests are, then make AmpStoredRequests a duplicate of StoredRequests using Amp values for generic fields
	if cfg.AmpStoredRequests.HTTP.Endpoint == "" && cfg.AmpStoredRequests.HTTPEvents.Endpoint == "" && cfg.AmpStoredRequests.Postgres.FetcherQueries.AmpQueryTemplate == "" && cfg.AmpStoredRequests.Postgres.PollUpdates.AmpQuery == "" {
		if cfg.StoredRequests.HTTP.AmpEndpoint != "" || cfg.StoredRequests.HTTPEvents.AmpEndpoint != "" || cfg.StoredRequests.Postgres.FetcherQueries.AmpQueryTemplate != "" || cfg.StoredRequests.Postgres.PollUpdates.AmpQuery != "" {
			cfg.AmpStoredRequests = cfg.StoredRequests
			cfg.AmpStoredRequests.HTTP.Endpoint = cfg.StoredRequests.HTTP.AmpEndpoint
			cfg.AmpStoredRequests.HTTPEvents.Endpoint = cfg.StoredRequests.HTTPEvents.AmpEndpoint
			cfg.AmpStoredRequests.Postgres.FetcherQueries.QueryTemplate = cfg.StoredRequests.Postgres.FetcherQueries.AmpQueryTemplate
			cfg.AmpStoredRequests.Postgres.PollUpdates.Query = cfg.StoredRequests.Postgres.PollUpdates.AmpQuery
		}
	}

	// Set default cache event endpoint names if not given
	if cfg.StoredRequests.CacheEventsAPI && cfg.StoredRequests.CacheEvents.Endpoint == "" {
		cfg.StoredRequests.CacheEvents.Endpoint = "/storedrequests/openrtb2"
	}
	if cfg.AmpStoredRequests.CacheEventsAPI && cfg.AmpStoredRequests.CacheEvents.Endpoint == "" {
		cfg.AmpStoredRequests.CacheEvents.Endpoint = "/storedrequests/amp"
	}
	if cfg.CategoryMapping.CacheEventsAPI && cfg.CategoryMapping.CacheEvents.Endpoint == "" {
		cfg.CategoryMapping.CacheEvents.Endpoint = "/storedrequest/categorymapping"
	}
}

func addListeners(cache stored_requests.Cache, eventProducers []events.EventProducer) (shutdown func()) {
	listeners := make([]*events.EventListener, 0, len(eventProducers))

	for _, ep := range eventProducers {
		listener := events.SimpleEventListener()
		go listener.Listen(cache, ep)
		listeners = append(listeners, listener)
	}

	return func() {
		for _, l := range listeners {
			l.Stop()
		}
	}
}

func newFetcher(cfg *config.StoredRequests, client *http.Client, db *sql.DB) (fetcher stored_requests.AllFetcher) {
	idList := make(stored_requests.MultiFetcher, 0, 3)

	if cfg.Files {
		fFetcher := newFilesystem(cfg.Path)
		idList = append(idList, fFetcher)
	}
	if cfg.Postgres.FetcherQueries.QueryTemplate != "" {
		glog.Infof("Loading Stored Requests via Postgres.\nQuery: %s", cfg.Postgres.FetcherQueries.QueryTemplate)
		idList = append(idList, db_fetcher.NewFetcher(db, cfg.Postgres.FetcherQueries.MakeQuery))
	}
	if cfg.HTTP.Endpoint != "" {
		glog.Infof("Loading Stored Requests via HTTP. endpoint=%s", cfg.HTTP.Endpoint)
		idList = append(idList, http_fetcher.NewFetcher(client, cfg.HTTP.Endpoint))
	}

	fetcher = consolidate(idList)
	return
}

func newCache(cfg *config.StoredRequests) stored_requests.Cache {
	if cfg.InMemoryCache.Type == "none" {
		glog.Info("No Stored Request cache configured. The Fetcher backend will be used for all Stored Requests.")
		return &nil_cache.NilCache{}
	}

	return memory.NewCache(&cfg.InMemoryCache)
}

func newEventProducers(cfg *config.StoredRequests, client *http.Client, db *sql.DB, router *httprouter.Router) (eventProducers []events.EventProducer) {
	if cfg.CacheEventsAPI {
		eventProducers = append(eventProducers, newEventsAPI(router, cfg.CacheEvents.Endpoint))
	}
	if cfg.HTTPEvents.RefreshRate != 0 {
		if cfg.HTTPEvents.Endpoint != "" {
			eventProducers = append(eventProducers, newHttpEvents(client, cfg.HTTPEvents.TimeoutDuration(), cfg.HTTPEvents.RefreshRateDuration(), cfg.HTTPEvents.Endpoint))
		}
	}
	if cfg.Postgres.CacheInitialization.Query != "" {
		// Make sure we don't miss any updates in between the initial fetch and the "update" polling.
		updateStartTime := time.Now()
		timeout := time.Duration(cfg.Postgres.CacheInitialization.Timeout) * time.Millisecond
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		eventProducers = append(eventProducers, postgresEvents.LoadAll(ctx, db, cfg.Postgres.CacheInitialization.Query))
		cancel()

		if cfg.Postgres.PollUpdates.Query != "" {
			eventProducers = append(eventProducers, newPostgresPolling(cfg.Postgres.PollUpdates, db, updateStartTime))
		}
	}
	return
}

func newPostgresPolling(cfg config.PostgresUpdatePolling, db *sql.DB, startTime time.Time) events.EventProducer {
	timeout := time.Duration(cfg.Timeout) * time.Millisecond
	ctxProducer := func() (ctx context.Context, canceller func()) {
		return context.WithTimeout(context.Background(), timeout)
	}
	return postgresEvents.PollForUpdates(ctxProducer, db, cfg.Query, startTime, time.Duration(cfg.RefreshRate)*time.Second)
}

func newEventsAPI(router *httprouter.Router, endpoint string) events.EventProducer {
	producer, handler := apiEvents.NewEventsAPI()
	router.POST(endpoint, handler)
	router.DELETE(endpoint, handler)
	return producer
}

func newHttpEvents(client *http.Client, timeout time.Duration, refreshRate time.Duration, endpoint string) events.EventProducer {
	ctxProducer := func() (ctx context.Context, canceller func()) {
		return context.WithTimeout(context.Background(), timeout)
	}
	return httpEvents.NewHTTPEvents(client, endpoint, ctxProducer, refreshRate)
}

func newFilesystem(configPath string) stored_requests.AllFetcher {
	glog.Infof("Loading Stored Requests from filesystem at path %s", configPath)
	fetcher, err := file_fetcher.NewFileFetcher(configPath)
	if err != nil {
		glog.Fatalf("Failed to create a FileFetcher: %v", err)
	}
	return fetcher
}

func newPostgresDB(cfg config.PostgresConnection) *sql.DB {
	db, err := sql.Open("postgres", cfg.ConnString())
	if err != nil {
		glog.Fatalf("Failed to open postgres connection: %v", err)
	}

	if err := db.Ping(); err != nil {
		glog.Fatalf("Failed to ping postgres: %v", err)
	}

	return db
}

// consolidate returns a single Fetcher from an array of fetchers of any size.
func consolidate(fetchers []stored_requests.AllFetcher) stored_requests.AllFetcher {
	if len(fetchers) == 0 {
		glog.Warning("No Stored Request support configured. request.imp[i].ext.prebid.storedrequest will be ignored. If you need this, check your app config")
		return empty_fetcher.EmptyFetcher{}
	} else if len(fetchers) == 1 {
		return fetchers[0]
	} else {
		return stored_requests.MultiFetcher(fetchers)
	}
}
