package mercure

import (
	"context"
	"github.com/go-redis/redis/v8"
	"net/url"
	"sync"
	"fmt"
	"encoding/json"
	"go.uber.org/zap"
	"time"
	"strconv"
)

func init() { //nolint:gochecknoinits
	RegisterTransportFactory("redis", NewRedisTCPTransport)
	RegisterTransportFactory("rediss", NewRedisTCPTransport)
	RegisterTransportFactory("redis+unix", NewRedisUnixTransport)
}

// RedisTransport implements the TransportInterface for redis databases
type RedisTransport struct {
	sync.RWMutex
	subscribers     *SubscriberList
	logger		Logger
	client		*redis.Client
	ctx		context.Context
	stream		string
	closed          chan struct{}
	closedOnce      sync.Once
	eventTTL	time.Duration
	cleanupInterval time.Duration
}

// Create a redis transport from the given URL and logger.  Before returning,
// remove any transport-specific query parameters from the URL.
func makeRedisTransport(u *url.URL, l Logger) (*RedisTransport, error) {
	var (
		eventTTL time.Duration = 24 * time.Hour
		cleanupInterval time.Duration
	)
	stream := "mercure"

	q := u.Query()

	if s := q.Get("stream"); s != "" {
		stream = s
		q.Del("stream");
	}

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

	return &RedisTransport{
		subscribers: NewSubscriberList(1e5),
		logger: l,
		ctx: context.TODO(),
		stream: stream,
		closed: make(chan struct{}),
		eventTTL: eventTTL,
		cleanupInterval: cleanupInterval,
	}, nil
}

// Connect to the given redis URL.
func (t *RedisTransport) connect(u *url.URL) error {
	options, err := redis.ParseURL(u.String())
	if err != nil {
		return err
	}

	if c := t.logger.Check(zap.DebugLevel, "Connecting"); c != nil {
		c.Write(zap.String("URL", u.String()))
	}

	client := redis.NewClient(options)
	if err := client.Ping(t.ctx).Err(); err != nil {
		return fmt.Errorf("Failed to connect to Redis: %w", err)
	}
	t.client = client
	return nil
}

// Create a TCP-based transport.
// Input URL is one of
//
//    redis://[user[:pass]@]HOST:[PORT][/DBNUM][?[stream=NAME][cleanup_interval=D[&event_ttl=D]][&REDIS_PARAM]]
//      Creates a plaintext TCP transport
//    rediss://[user[:pass]@]HOST:[PORT][/DBNUM][?[stream=NAME][cleanup_interval=D[&event_ttl=D]][&REDIS_PARAM]]
//      Creates TLS-encrypted TCP transport
//
// HOST is the hostname or IP address of the redis server.
//
// PORT is the port number it is listening on.  It defaults to 6379.
//
// DBNUM is the number of the redis database to use
//
// STREAM sets the redis stream name (defaults to "mercure")
//
// The parameters cleanup_interval and event_ttl control periodic database cleanups.  Both take as their
// argument a duration specification suitable as input to time.ParseDuration.  The cleanup_control parameter
// sets a duration between two successive database cleanups.  It must be set in order for the cleanup
// routine to be enabled.  Optional event_ttl parameter sets the time-to-live of an event in the
// database.  It defaults to 24 hours.
//
// REDIS_PARAM are redis-specific parameters as described in
//     https://pkg.go.dev/github.com/go-redis/redis/v8#ParseURL.
//
func NewRedisTCPTransport(iu *url.URL, l Logger, tss *TopicSelectorStore) (Transport, error) {
	u, err := url.Parse(iu.String())
	if err != nil {
		return nil, fmt.Errorf("can't clone URL: %w", err)
	}

	rt, err := makeRedisTransport(u, l);
	if err != nil {
		return nil, err
	}

	if err := rt.connect(u); err != nil {
		return nil, err
	}

	if rt.cleanupInterval > 0 {
		go rt.cleanup()
	}

	return rt, nil
}

// Create a UNIX-based (socket) redis transport.
// Input URL is:
//
//     redis+unix://[user[:pass]@]SOCKET_PATH[?[stream=NAME][cleanup_interval=N[&event_ttl=N]][&db=DBNUM][REDIS_PARAM]]
//
// SOCKET_PATH is the pathname of the unix socket server is listening on.  It must be an absolute pathname,
// i.e. it must begine with a /.
//
// DBNUM is the number of the redis database to use.
//
// See above for the description of stream, cleanup_interval and event_ttl.
//
// REDIS_PARAM are redis-specific parameters as described in
//     https://pkg.go.dev/github.com/go-redis/redis/v8#ParseURL.
//
func NewRedisUnixTransport(iu *url.URL, l Logger, tss *TopicSelectorStore) (Transport, error) {
	u, err := url.Parse(iu.String())
	if err != nil {
		return nil, fmt.Errorf("can't clone URL: %w", err)
	}

	rt, err := makeRedisTransport(u, l);
	if err != nil {
		return nil, err
	}
	u.Scheme = "unix"

	if err := rt.connect(u); err != nil {
		return nil, err
	}

	if rt.cleanupInterval > 0 {
		go rt.cleanup()
	}

	return rt, nil
}

func (t *RedisTransport) storeUpdate(update *Update) error {
	updateJSON, err := json.Marshal(*update)
	if err != nil {
		return fmt.Errorf("error marshalling update: %w", err)
	}
	id, err := t.client.XAdd(t.ctx, &redis.XAddArgs{
		Stream: t.stream,
		ID:     "*",
		Values: map[string]interface{}{"update": updateJSON},
	}).Result()
	if err == nil {
		err = t.client.Set(t.ctx, update.ID, id, 0).Err()
	}
	if c := t.logger.Check(zap.DebugLevel, "Storing update"); c != nil {
		c.Write(zap.String("updateJSON", string(updateJSON)),
			zap.String("Redis ID", id),
			zap.Error(err))
	}

	return err
}

func (t *RedisTransport) Dispatch(update *Update) error {
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

func (t *RedisTransport) dispatchHistory(s *Subscriber) (err error) {
	key := "0-0"
	val, e := t.client.Get(t.ctx, s.RequestLastEventID).Result()
	if e == nil {
		key = val
	} else if e != redis.Nil {
		if c := t.logger.Check(zap.ErrorLevel, "Can't find RequestLastEventID"); c != nil {
			c.Write(zap.String("RequestLastEventID", s.RequestLastEventID))
		}
	}

	if dbg := t.logger.Check(zap.DebugLevel, "dispatchHistory"); dbg != nil {
		dbg.Write(zap.String("RequestLastEventID", s.RequestLastEventID),
			  zap.String("key", key))
	}

	res, e := t.client.XReadStreams(t.ctx, t.stream, key).Result()
	if e != nil {
		err = fmt.Errorf("XREAD error: %w", e)
		return
	}

	if dbg := t.logger.Check(zap.DebugLevel, "dispatchHistory"); dbg != nil {
		dbg.Write(zap.Int("Message count", len(res[0].Messages)))
	}

	responseLastEventID := EarliestLastEventID

	for _, msg := range res[0].Messages {
		var update *Update
		v, ok := msg.Values[`update`]
		if !ok {
			err = fmt.Errorf("Malformed update message for %s", key)
			break
		}
		if upds, ok := v.(string); ok {
			if dbg := t.logger.Check(zap.DebugLevel, "dispatchHistory"); dbg != nil {
				dbg.Write(zap.String("Update", upds))
			}
			if err = json.Unmarshal([]byte(upds), &update); err != nil {
				break
			}

			if s.Match(update) && s.Dispatch(update, true) {
				responseLastEventID = msg.ID
			}
		} else {
			err = fmt.Errorf("Bad update type for %s", key)
			break
		}
	}
	if dbg := t.logger.Check(zap.DebugLevel, "dispatchHistory"); dbg != nil {
		dbg.Write(zap.String("responseLastEventID", responseLastEventID))
	}
	s.HistoryDispatched(responseLastEventID)
	return err
}

// AddSubscriber adds a new subscriber to the transport.
func (t *RedisTransport) AddSubscriber(s *Subscriber) error {
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
func (t *RedisTransport) RemoveSubscriber(s *Subscriber) error {
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
func (t *RedisTransport) Close() (err error) {
	t.closedOnce.Do(func() {
		close(t.closed)

		t.Lock()
		defer t.Unlock()

		t.subscribers.Walk(0, func(s *Subscriber) bool {
			s.Disconnect()
			return true
		})
		err = t.client.Close()
	})

	return
}

func (t *RedisTransport) lastEventID() (result string, err error) {
	result = EarliestLastEventID // Set default result value
	res, e := t.client.XInfoStream(t.ctx, "stream").Result()
	if e == nil {
		val, e := t.client.Get(t.ctx, res.LastGeneratedID).Result()
		if e == nil {
			result = val
		} else if e != redis.Nil {
			err = e
		}
	} else {
		err = e
	}
	return
}

// GetSubscribers gets the list of active subscribers.
func (t *RedisTransport) GetSubscribers() (string, []*Subscriber, error) {
	t.RLock()
	defer t.RUnlock()

	var subscribers []*Subscriber
	t.subscribers.Walk(0, func(s *Subscriber) bool {
		subscribers = append(subscribers, s)

		return true
	})

	lastEventID, err := t.lastEventID()
	if err != nil {
		if c := t.logger.Check(zap.ErrorLevel, "Can't find LastEventID"); c != nil {
			c.Write(zap.Error(err))
		}
	}
	return lastEventID, subscribers, nil
}

func (t *RedisTransport) trim() error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	t.Lock()
	defer t.Unlock()

	servertime, err := t.client.Time(t.ctx).Result()
	if err != nil {
		if c := t.logger.Check(zap.ErrorLevel, "Can't get server time"); c != nil {
			c.Write(zap.Error(err))
		}
		return nil
	}

	minid := strconv.FormatInt(servertime.Add(- t.eventTTL).UnixNano() / 1e6, 10)
	if c := t.logger.Check(zap.DebugLevel, "Redis minID"); c != nil {
		c.Write(zap.Time("now", servertime),
			zap.String("minID", minid))
	}
	res, err := t.client.XRange(t.ctx, t.stream, "0", minid).Result()
	if err != nil {
		if err == redis.Nil {
			if c := t.logger.Check(zap.DebugLevel, "Redis trim"); c != nil {
				c.Write(zap.String("message", "nothing to trim"))
			}
		} else if c := t.logger.Check(zap.ErrorLevel, "Can't find minID for trimming"); c != nil {
			c.Write(zap.String("minID", minid), zap.Error(err))
		}
		return nil
	}

	var ids []string
	for _, msg := range res {
		var update *Update
		v, ok := msg.Values[`update`]
		if !ok {
			if c := t.logger.Check(zap.ErrorLevel, "Malformed update message"); c != nil {
				c.Write(zap.String("ID", msg.ID))
			}
		} else if upds, ok := v.(string); ok {
			if err = json.Unmarshal([]byte(upds), &update); err != nil {
				if c := t.logger.Check(zap.ErrorLevel, "Unmarshal error"); c != nil {
					c.Write(zap.String("ID", msg.ID),
						zap.String("update", upds),
						zap.Error(err))
				}
			} else {
				ids = append(ids, update.ID)
			}
		}
	}

	nIDs := int64(0)
	nEvts := int64(0)

	if (len(ids) > 0) {
		if nIDs, err = t.client.Del(t.ctx, ids...).Result(); err != nil {
			if c := t.logger.Check(zap.ErrorLevel, "Deleting IDs"); c != nil {
				c.Write(zap.Int("Total IDs", len(ids)),
					zap.Int64("Deleted IDs", nIDs),
					zap.Error(err))
			}
		}
	}

	if nEvts, err = t.client.XTrimMinID(t.ctx, t.stream, minid).Result(); err != nil {
		if c := t.logger.Check(zap.ErrorLevel, "Deleting entries"); c != nil {
			c.Write(zap.Error(err))
		}
	}

	if c := t.logger.Check(zap.InfoLevel, "Redis trim"); c != nil {
		c.Write(zap.Int64("IDs deleted", nIDs),
			zap.Int64("Events deleted", nEvts))
	}

	return nil
}

func (t *RedisTransport) cleanup() {
	if c := t.logger.Check(zap.DebugLevel, "Redis cleanup"); c != nil {
		c.Write(zap.Duration("cleanupInterval", t.cleanupInterval),
			zap.Duration("envenTTL", t.eventTTL))
	}
	for t.trim() == nil {
		time.Sleep(t.cleanupInterval)
	}
}

// Interface guards.
var (
	_ Transport            = (*RedisTransport)(nil)
	_ TransportSubscribers = (*RedisTransport)(nil)
)
