package mercure

import (
	"database/sql"
	"net/url"
	"time"
	"sync"
	"fmt"
	"encoding/json"
	"go.uber.org/zap"
	_ "github.com/lib/pq"
	"errors"
)

func init() { //nolint:gochecknoinits
	RegisterTransportFactory("postgres", NewPostgresTransport)
}

// PostgresTransport implements the TransportInterface for PostgreSQL databases
type PostgresTransport struct {
	sync.RWMutex
	subscribers     *SubscriberList
	logger		Logger
	db		*sql.DB
	closed          chan struct{}
	closedOnce      sync.Once
	eventTTL	time.Duration
	cleanupInterval time.Duration
}

func NewPostgresTransport(iu *url.URL, l Logger, tss *TopicSelectorStore) (Transport, error) {
	var (
		eventTTL time.Duration = 24 * time.Hour
		cleanupInterval time.Duration
	)

	u, err := url.Parse(iu.String())
	if err != nil {
		return nil, fmt.Errorf("can't clone URL: %w", err)
	}

	q := u.Query()

	if s := q.Get("event_ttl"); s != "" {
		d, err := time.ParseDuration(s)
		if err == nil {
			if d < 0 {
				eventTTL = - d
			} else {
				eventTTL = d
			}
		} else if c := l.Check(zap.ErrorLevel, "unparsable redis event TTL"); c != nil {
			c.Write(zap.String("event-ttl", s))
		}
		q.Del("event_ttl")
	}

	if s := q.Get("cleanup_interval"); s != "" {
		d, err := time.ParseDuration(s)
		if err == nil {
			cleanupInterval = d
		} else if c := l.Check(zap.ErrorLevel, "unparsable redis cleanup interval"); c != nil {
			c.Write(zap.String("cleanup-interval", s))
		}
		q.Del("cleanup_interval")
	}
	u.RawQuery = q.Encode()

	db, err := sql.Open("postgres", iu.String())
	if err != nil {
		return nil, fmt.Errorf("can't parse URL: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("can't connect to the database: %w", err)
	}

	return &PostgresTransport{
		subscribers: NewSubscriberList(1e5),
		logger: l,
		db: db,
		closed: make(chan struct{}),
		eventTTL: eventTTL,
		cleanupInterval: cleanupInterval,
	}, nil
}

func (t *PostgresTransport) storeUpdate(update *Update) error {
	updateJSON, err := json.Marshal(*update)
	if err != nil {
		return fmt.Errorf("error marshalling update: %w", err)
	}
	if _, err := t.db.Exec("INSERT INTO EVENTS(id,message) values($1,$2)", update.ID, updateJSON); err != nil {
		return fmt.Errorf("error inserting update: %w", err)
	}
	return nil
}

func (t *PostgresTransport) Dispatch(update *Update) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	t.Lock()
	defer t.Unlock()

	AssignUUID(update)
	if err := t.storeUpdate(update); err != nil {
		return err
	}

	for _, s := range t.subscribers.MatchAny(update) {
		s.Dispatch(update, false)
	}

	return nil
}

func (t *PostgresTransport) dispatchHistory(s *Subscriber) error {
	var ts time.Time
	err := t.db.QueryRow(`SELECT ts from events where id = $1`, s.RequestLastEventID).Scan(&ts)

	var rows *sql.Rows
	if err == nil {
		rows, err = t.db.Query(`SELECT message FROM events WHERE ts > $1 ORDER BY ts`,
			ts.Format(`2006-01-02T15:04:05.999999`))
	} else if errors.Is(err, sql.ErrNoRows) {
		rows, err = t.db.Query(`SELECT message FROM events ORDER BY ts`)
	} else {
		return fmt.Errorf("Error getting timestamp: %w", err)
	}

	responseLastEventID := EarliestLastEventID
	if err == nil {
		for rows.Next() {
			var (
				message string
				update *Update
			)
			if err := rows.Scan(&message); err != nil {
				if c := t.logger.Check(zap.ErrorLevel, "Scan error"); c != nil {
					c.Write(zap.Error(err))
				}
				break
			}
			if err := json.Unmarshal([]byte(message), &update); err != nil {
				if c := t.logger.Check(zap.ErrorLevel, "Bad JSON"); c != nil {
					c.Write(zap.String("message", message),
						zap.Error(err))
				}
				break
			}
			if s.Match(update) && s.Dispatch(update, true) {
				responseLastEventID = update.ID
			}
		}

	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	s.HistoryDispatched(responseLastEventID)

	return nil
}

// AddSubscriber adds a new subscriber to the transport.
func (t *PostgresTransport) AddSubscriber(s *Subscriber) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	if dbg := t.logger.Check(zap.DebugLevel, "AddSubscriber"); dbg != nil {
		dbg.Write(zap.String("ID", s.ID),
			  zap.String("s.RequestLastEventID", s.RequestLastEventID))
	}
	t.Lock()
	t.subscribers.Add(s)
	t.Unlock()

	if s.RequestLastEventID != "" {
		if err := t.dispatchHistory(s); err != nil {
			if c := t.logger.Check(zap.ErrorLevel, "Dispatch error"); c != nil {
				c.Write(zap.Error(err))
			}
			return err
		}
	}

	s.Ready()
	return nil
}

// RemoveSubscriber removes a new subscriber from the transport.
func (t *PostgresTransport) RemoveSubscriber(s *Subscriber) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	t.Lock()
	defer t.Unlock()
	t.subscribers.Remove(s)

	return nil
}

// Close closes the Transport.
func (t *PostgresTransport) Close() (err error) {
	t.closedOnce.Do(func() {
		close(t.closed)

		t.Lock()
		defer t.Unlock()

		t.subscribers.Walk(0, func(s *Subscriber) bool {
			s.Disconnect()
			return true
		})
		err = t.db.Close()
	})

	return
}

func (t *PostgresTransport) lastEventID() (result string, err error) {
	err = t.db.QueryRow(`SELECT e1.id FROM events e1 INNER JOIN (SELECT MAX(ts) ts FROM events) e2 ON e1.ts = e2.ts`).Scan(&result)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		result = EarliestLastEventID
	}
	return
}

// GetSubscribers gets the list of active subscribers.
func (t *PostgresTransport) GetSubscribers() (string, []*Subscriber, error) {
	t.RLock()
	defer t.RUnlock()

	var subscribers []*Subscriber
	t.subscribers.Walk(0, func(s *Subscriber) bool {
		subscribers = append(subscribers, s)
		return true
	})

	lastEventID, err := t.lastEventID()
	return lastEventID, subscribers, err
}


// Interface guards.
var (
	_ Transport            = (*PostgresTransport)(nil)
	_ TransportSubscribers = (*PostgresTransport)(nil)
)
