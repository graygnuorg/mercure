// +build gdbm

package mercure

import (
	"encoding/json"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/graygnuorg/go-gdbm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func createGdbmTransport(t *testing.T, dsn string, debug bool) *GdbmTransport {
	u, _ := url.Parse(dsn)
	var logger Logger
	if debug {
		logger, _ = zap.NewDevelopment()
	} else {
		logger = zap.NewNop()
	}
	transport, err := NewGdbmTransport(u, logger, nil)
	assert.Nil(t, err)
	return transport.(*GdbmTransport)
}

func TestGdbmTransportHistory(t *testing.T) {
	os.Remove("test.db")
	transport := createGdbmTransport(t, "gdbm://test.db", false)
	defer transport.Close()
	defer os.Remove("test.db")

	topics := []string{"https://example.com/foo"}
	for i := 1; i <= 10; i++ {
		transport.Dispatch(&Update{
			Event:  Event{ID: strconv.Itoa(i)},
			Topics: topics,
		})
	}

	s := NewSubscriber("8", transport.logger)
	s.SetTopics(topics, nil)

	require.Nil(t, transport.AddSubscriber(s))

	for i := 9; i <= 10; i++ {
		assert.Equal(t, strconv.Itoa(i), (<-s.Receive()).ID)
	}
}

func TestGdbmTopicSelectorHistory(t *testing.T) {
	os.Remove("test.db")
	transport := createGdbmTransport(t, "gdbm://test.db", false)
	defer transport.Close()
	defer os.Remove("test.db")

	transport.Dispatch(&Update{Topics: []string{"http://example.com/subscribed"}, Event: Event{ID: "1"}})
	transport.Dispatch(&Update{Topics: []string{"http://example.com/not-subscribed"}, Event: Event{ID: "2"}})
	transport.Dispatch(&Update{Topics: []string{"http://example.com/subscribed-public-only"}, Private: true, Event: Event{ID: "3"}})
	transport.Dispatch(&Update{Topics: []string{"http://example.com/subscribed-public-only"}, Event: Event{ID: "4"}})

	s := NewSubscriber(EarliestLastEventID, transport.logger)
	s.SetTopics([]string{"http://example.com/subscribed", "http://example.com/subscribed-public-only"}, []string{"http://example.com/subscribed"})

	require.Nil(t, transport.AddSubscriber(s))

	assert.Equal(t, "1", (<-s.Receive()).ID)
	assert.Equal(t, "4", (<-s.Receive()).ID)
}

func TestGdbmTransportRetrieveAllHistory(t *testing.T) {
	os.Remove("test.db")
	transport := createGdbmTransport(t, "gdbm://test.db", false)
	defer transport.Close()
	defer os.Remove("test.db")

	topics := []string{"https://example.com/foo"}
	for i := 1; i <= 10; i++ {
		transport.Dispatch(&Update{
			Event:  Event{ID: strconv.Itoa(i)},
			Topics: topics,
		})
	}

	s := NewSubscriber(EarliestLastEventID, transport.logger)
	s.SetTopics(topics, nil)
	require.Nil(t, transport.AddSubscriber(s))

	for i := 1; i <= 10; i++ {
		assert.Equal(t, strconv.Itoa(i), (<-s.Receive()).ID)
	}
}

func TestGdbmTransportHistoryAndLive(t *testing.T) {
	os.Remove("test.db")
	transport := createGdbmTransport(t, "gdbm://test.db", false)
	defer transport.Close()
	defer os.Remove("test.db")

	topics := []string{"https://example.com/foo"}
	for i := 1; i <= 10; i++ {
		transport.Dispatch(&Update{
			Topics: topics,
			Event:  Event{ID: strconv.Itoa(i)},
		})
	}

	s := NewSubscriber("8", transport.logger)
	s.SetTopics(topics, nil)
	require.Nil(t, transport.AddSubscriber(s))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 9; i <= 11; i++ {
			assert.Equal(t, strconv.Itoa(i), (<-s.Receive()).ID)
		}
	}()

	transport.Dispatch(&Update{
		Event:  Event{ID: "11"},
		Topics: topics,
	})

	wg.Wait()
}

func TestGdbmTransportPurgeHistory(t *testing.T) {
	topics := []string{"https://example.com/foo"}

	db, err := gdbm.Open("test.db", gdbm.ModeNewdb)
	assert.Nil(t, err)

	rts := []int{
		110,
		100,
		92,
		25,
		20,
		14,
		10,
	}

	now := time.Now()

	for i, r := range rts {
		entry := gdbmEntry{
			Update: Update{
				Event: Event{ID: strconv.Itoa(i+1)},
				Topics: topics,
			},
			Timestamp: now.Add(- time.Duration(r * 1e9)),
			Next: strconv.Itoa(i+2),
		}
		if i == len(rts)-1 {
			entry.Next = ""
		}
		js, err := json.Marshal(entry)
		assert.Nil(t, err)
		err = db.Store([]byte(entry.Update.ID), js, false)
		assert.Nil(t, err)
	}

	meta := metaInfo{
		Head: "1",
		Tail: strconv.Itoa(len(rts)),
	}

	js, err := json.Marshal(meta)
	assert.Nil(t, err)

	err = db.Store(metaInfoKey, js, false)
	assert.Nil(t, err)

	db.Close()

	transport := createGdbmTransport(t, "gdbm://test.db?event_ttl=20s", false)
	defer transport.Close()
	defer os.Remove("test.db")

	err = transport.trim()
	assert.Nil(t, err)

	meta, err = transport.getMetaInfo()
	assert.Nil(t, err)

	assert.Equal(t, "6", meta.Head)
	assert.Equal(t, strconv.Itoa(len(rts)), meta.Tail)
}
