package mercure

import (
	"net/url"
	"errors"
	"github.com/graygnuorg/go-gdbm"
	"sync"
	"fmt"
	"encoding/json"
	"go.uber.org/zap"
	"time"
	//"strconv"
)

func init() { //nolint:gochecknoinits
	RegisterTransportFactory("gdbm", NewGdbmTransport)
}

type GdbmTransport struct {
	sync.RWMutex
	subscribers     *SubscriberList
	logger		Logger
	db		*gdbm.Database
	closed          chan struct{}
	closedOnce      sync.Once
	eventTTL	time.Duration
	cleanupInterval time.Duration
}

func NewGdbmTransport(u *url.URL, l Logger, tss *TopicSelectorStore) (Transport, error) { //nolint:ireturn
	var (
		eventTTL time.Duration = 24 * time.Hour
		cleanupInterval time.Duration
	)
	q := u.Query()
	if s := q.Get("event_ttl"); s != "" {
		d, err := time.ParseDuration(s)
		if err == nil {
			if d < 0 {
				eventTTL = - d
			} else {
				eventTTL = d
			}
		} else if c := l.Check(zap.ErrorLevel, "unparsable gdbm event TTL"); c != nil {
			c.Write(zap.String("event-ttl", s))
		}
	}
	if s := q.Get("cleanup_interval"); s != "" {
		d, err := time.ParseDuration(s)
		if err == nil {
			cleanupInterval = d
		} else if c := l.Check(zap.ErrorLevel, "unparsable gdbm cleanup interval"); c != nil {
			c.Write(zap.String("cleanup-interval", s))
		}
	}

	path := u.Path
	if path == "" {
		path = u.Host // relative path (gdbm://path.db)
	} // else absolute path (gdbm:///path.db)

	if path == "" {
		return nil, &TransportError{u.Redacted(), "missing path", nil}
	}

	db, err := gdbm.Open(path, gdbm.ModeWrcreat)
	if err != nil {
		return nil, &TransportError{dsn: u.Redacted(), err: err}
	}

	t := &GdbmTransport{
		subscribers: NewSubscriberList(1e5),
		logger: l,
		db: db,
		closed: make(chan struct{}),
		eventTTL: eventTTL,
		cleanupInterval: cleanupInterval,
	}

	if t.cleanupInterval > 0 {
		go t.cleanup()
	}

	return t, nil
}

type metaInfo struct {
	Head	string
	Tail	string
}

var metaInfoKey = []byte("meta_info")

func (t *GdbmTransport) getMetaInfo() (meta metaInfo, err error) {
	var val []byte
	val, err = t.db.Fetch(metaInfoKey)
	if err == nil {
		err = json.Unmarshal(val, &meta)
	} else if errors.Is(err, gdbm.ErrItemNotFound) {
		meta.Head = ""
		meta.Tail = ""
		err = nil
	}
	return
}

func (t *GdbmTransport) storeMetaInfo(meta metaInfo) error {
	mb, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return t.db.Store(metaInfoKey, mb, true)
}

type gdbmEntry struct {
	Update	  Update
	Timestamp time.Time
	Next	  string
}

func (t *GdbmTransport) getEntry(id string) (ent gdbmEntry, err error) {
	var val []byte
	val, err = t.db.Fetch([]byte(id))
	if err == nil {
		err = json.Unmarshal(val, &ent);
	}
	return
}

func (t *GdbmTransport) storeEntry(ent gdbmEntry, replace bool) error {
	js, err := json.Marshal(ent)
	if err != nil {
		return fmt.Errorf("error marshalling entry: %w", err)
	}

	return t.db.Store([]byte(ent.Update.ID), js, replace);
}

func (t *GdbmTransport) storeUpdate(update *Update) error {
	if dbg := t.logger.Check(zap.DebugLevel, "dispatchHistory"); dbg != nil {
		dbg.Write(zap.String("ID", update.ID))
	}

	ent := gdbmEntry{
		Update: *update,
		Timestamp: time.Now(),
		Next: "",
	}
	if err := t.storeEntry(ent, false); err != nil {
		return fmt.Errorf("error storing entry %s: %w", update.ID, err)
	}

	meta, err := t.getMetaInfo()
	if err != nil {
		return fmt.Errorf("error getting meta-info during update: %w", err)
	}
	if meta.Tail == "" {
		meta.Head = update.ID
	} else {
		prev, err := t.getEntry(meta.Tail)
		if err == nil {
			prev.Next = update.ID
			if err = t.storeEntry(prev, true); err != nil {
				return err
			}
		} else if errors.Is(err, gdbm.ErrItemNotFound) {
			//FIXME?
			return err
		}
	}
	if dbg := t.logger.Check(zap.DebugLevel, "dispatchHistory"); dbg != nil {
		dbg.Write(zap.String("Tail", update.ID))
	}
	meta.Tail = update.ID
	return t.storeMetaInfo(meta)
}

func (t *GdbmTransport) Dispatch(update *Update) error {
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

func (t *GdbmTransport) dispatchHistory(s *Subscriber) error {
	lastID := s.RequestLastEventID
	ent, err := t.getEntry(lastID)
	if err == nil {
		lastID = ent.Next
	} else if errors.Is(err, gdbm.ErrItemNotFound) {
		meta, err := t.getMetaInfo()
		if err != nil {
			return fmt.Errorf("can't get GDBM meta-info: %w", err)
		}
		lastID = meta.Head
	} else {
		return fmt.Errorf("can't get last event entry: %w", err)
	}

	if dbg := t.logger.Check(zap.DebugLevel, "dispatchHistory"); dbg != nil {
		dbg.Write(zap.String("lastID", lastID))
	}

	responseLastEventID := EarliestLastEventID
	for lastID != "" {
		ent, err := t.getEntry(lastID)
		if err != nil {
			if c := t.logger.Check(zap.ErrorLevel, "Can't find entry"); c != nil {
				c.Write(zap.String("lastID", lastID), zap.Error(err))
			}
			break
		}
		if s.Match(&ent.Update) && s.Dispatch(&ent.Update, true) {
			responseLastEventID = lastID
		}
		lastID = ent.Next
	}
	if dbg := t.logger.Check(zap.DebugLevel, "dispatchHistory"); dbg != nil {
		dbg.Write(zap.String("responseLastEventID", responseLastEventID))
	}
	s.HistoryDispatched(responseLastEventID)
	return nil
}

// AddSubscriber adds a new subscriber to the transport.
func (t *GdbmTransport) AddSubscriber(s *Subscriber) error {
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
func (t *GdbmTransport) RemoveSubscriber(s *Subscriber) error {
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
func (t *GdbmTransport) Close() (err error) {
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

// GetSubscribers gets the list of active subscribers.
func (t *GdbmTransport) GetSubscribers() (string, []*Subscriber, error) {
	t.RLock()
	defer t.RUnlock()

	var subscribers []*Subscriber
	t.subscribers.Walk(0, func(s *Subscriber) bool {
		subscribers = append(subscribers, s)

		return true
	})

	meta, err := t.getMetaInfo()
	if err != nil {
		if c := t.logger.Check(zap.ErrorLevel, "Can't find LastEventID"); c != nil {
			c.Write(zap.Error(err))
		}
	}
	return meta.Tail, subscribers, nil
}

func (t *GdbmTransport) trim() error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	t.Lock()
	defer t.Unlock()

	meta, err := t.getMetaInfo()
	if err != nil {
		return fmt.Errorf("Can't find meta-info for trimming: %w", err)
	}

	id := meta.Head
	mintime := time.Now().Add(- t.eventTTL)
	if c := t.logger.Check(zap.DebugLevel, "trim"); c != nil {
		c.Write(zap.Time("minTime", mintime),
			zap.String("firstID", id))
	}

	var ids []string

	for id != "" {
		ent, err := t.getEntry(id)
		if err != nil {
			if errors.Is(err, gdbm.ErrItemNotFound) {
				break
			}
			return err
		}
		if ent.Timestamp.Before(mintime) {
			ids = append(ids, id)
			id = ent.Next
		} else {
			break
		}
	}

	if c := t.logger.Check(zap.DebugLevel, "trim"); c != nil {
		c.Write(zap.Int("IDs to remove", len(ids)))
	}

	for _, key := range ids {
		err := t.db.Delete([]byte(key))
		if err != nil && !errors.Is(err, gdbm.ErrItemNotFound) {
			if c := t.logger.Check(zap.ErrorLevel, "trim cannot delete entry"); c != nil {
				c.Write(zap.String("ID", key), zap.Error(err))
			}
			id = key
			break
		}
	}

	if id == "" {
		meta.Head = ""
		meta.Tail = ""
	} else {
		meta.Head = id
	}

	if c := t.logger.Check(zap.DebugLevel, "trim"); c != nil {
		c.Write(zap.String("Meta Head", meta.Head),
			zap.String("Meta Tail", meta.Tail))
	}

	err = t.storeMetaInfo(meta)
	if err != nil {
		if c := t.logger.Check(zap.ErrorLevel, "trim cannot store meta-info"); c != nil {
			c.Write(zap.Error(err))
		}
	}

	return nil
}

func (t *GdbmTransport) cleanup() {
	if c := t.logger.Check(zap.DebugLevel, "GDBM cleanup"); c != nil {
		c.Write(zap.Duration("cleanupInterval", t.cleanupInterval),
			zap.Duration("envenTTL", t.eventTTL))
	}
	for t.trim() == nil {
		time.Sleep(t.cleanupInterval)
	}
}


// Interface guards.
var (
	_ Transport            = (*GdbmTransport)(nil)
	_ TransportSubscribers = (*GdbmTransport)(nil)
)
