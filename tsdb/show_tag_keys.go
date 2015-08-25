package tsdb

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"

	"github.com/influxdb/influxdb/influxql"
)

// ShowTagKeysExecutor implements the Executor interface for a SHOW MEASUREMENTS statement.
type ShowTagKeysExecutor struct {
	stmt      *influxql.ShowTagKeysStatement
	mappers   []Mapper
	chunkSize int
}

// NewShowTagKeysExecutor returns a new ShowTagKeysExecutor.
func NewShowTagKeysExecutor(stmt *influxql.ShowTagKeysStatement, mappers []Mapper, chunkSize int) *ShowTagKeysExecutor {
	return &ShowTagKeysExecutor{
		stmt:      stmt,
		mappers:   mappers,
		chunkSize: chunkSize,
	}
}

// Execute begins execution of the query and returns a channel to receive rows.
func (e *ShowTagKeysExecutor) Execute() <-chan *influxql.Row {
	log.Println("ShowTagKeysExecutor.Execute: start")
	defer log.Println("ShowTagKeysExecutor.Execute: end")
	// Create output channel and stream data in a separate goroutine.
	out := make(chan *influxql.Row, 0)

	// It's important that all resources are released when execution completes.
	defer e.close()

	go func() {
		log.Printf("#mappers = %d", len(e.mappers))
		// Open the mappers.
		for _, m := range e.mappers {
			if err := m.Open(); err != nil {
				out <- &influxql.Row{Err: err}
				return
			}
		}

		// Create a set to hold measurement names from mappers.
		set := map[string]struct{}{}
		// Iterate through mappers collecting measurement names.
		for _, m := range e.mappers {
			// Get the data from the mapper.
			c, err := m.NextChunk()
			if err != nil {
				out <- &influxql.Row{Err: err}
				return
			} else if c == nil {
				// Mapper had no data.
				continue
			}

			// Convert the mapper chunk to a string array of measurement names.
			mms, ok := c.([]string)
			if !ok {
				out <- &influxql.Row{Err: fmt.Errorf("show tag keys mapper returned invalid type: %T", c)}
				return
			}

			// Add the measurement names to the set.
			for _, mm := range mms {
				set[mm] = struct{}{}
			}
		}

		// Convert the set into an array of measurement names.
		tagKeys := make([]string, 0, len(set))
		for mm := range set {
			tagKeys = append(tagKeys, mm)
		}
		// Sort the names.
		sort.Strings(tagKeys)

		// Calculate OFFSET and LIMIT
		off := e.stmt.Offset
		lim := len(tagKeys)
		stmtLim := e.stmt.Limit

		if stmtLim > 0 && off+stmtLim < lim {
			lim = off + stmtLim
		} else if off > lim {
			off, lim = 0, 0
		}

		// Put the results in a row and send it.
		row := &influxql.Row{
			Name:    "tagKey",
			Columns: []string{"name"},
			Values:  make([][]interface{}, 0, len(tagKeys)),
		}

		for _, m := range tagKeys[off:lim] {
			v := []interface{}{m}
			row.Values = append(row.Values, v)
		}

		if len(row.Values) > 0 {
			out <- row
		}

		close(out)
	}()
	return out
}

// Close closes the executor such that all resources are released. Once closed,
// an executor may not be re-used.
func (e *ShowTagKeysExecutor) close() {
	if e != nil {
		for _, m := range e.mappers {
			m.Close()
		}
	}
}

// ShowTagKeysMapper is a mapper for collecting measurement names from a shard.
type ShowTagKeysMapper struct {
	remote    Mapper
	shard     *Shard
	stmt      *influxql.ShowTagKeysStatement
	chunkSize int
	state     interface{}
}

// NewShowTagKeysMapper returns a mapper for the given shard, which will return data for the meta statement.
func NewShowTagKeysMapper(shard *Shard, stmt *influxql.ShowTagKeysStatement, chunkSize int) *ShowTagKeysMapper {
	return &ShowTagKeysMapper{
		shard:     shard,
		stmt:      stmt,
		chunkSize: chunkSize,
	}
}

// Open opens the mapper for use.
func (m *ShowTagKeysMapper) Open() error {
	log.Println("ShowTagKeysMapper.Open: start")
	defer log.Println("ShowTagKeysMapper.Open: end")
	if m.remote != nil {
		return m.remote.Open()
	}

	// Get measurements from sources in the statement if provided or database if not.
	measurements, err := measurementsFromSourcesOrDB(m.shard.index, m.stmt.Sources...)
	if err != nil {
		return err
	}

	// If a WHERE clause was specified, filter the measurements.
	if m.stmt.Condition != nil {
		var err error
		whereMs, err := m.shard.index.measurementsByExpr(m.stmt.Condition)
		if err != nil {
			return err
		}

		sort.Sort(whereMs)

		measurements = measurements.intersect(whereMs)
	}

	// Create a channel to send measurement names on.
	ch := make(chan string)
	// Start a goroutine to send the names over the channel as needed.
	go func() {
		for _, mm := range measurements {
			// Get the tag keys in sorted order.
			keys := mm.TagKeys()

			// Send the tag keys over the chan as needed.
			for _, k := range keys {
				ch <- k
			}
		}
		close(ch)
	}()

	// Store the channel as the state of the mapper.
	m.state = ch

	return nil
}

// SetRemote sets the remote mapper to use.
func (m *ShowTagKeysMapper) SetRemote(remote Mapper) error {
	m.remote = remote
	return nil
}

// TagSets is only implemented on this mapper to satisfy the Mapper interface.
func (m *ShowTagKeysMapper) TagSets() []string { return nil }

// Fields returns a list of field names for this mapper.
func (m *ShowTagKeysMapper) Fields() []string { return []string{"name"} }

// NextChunk returns the next chunk of measurement names.
func (m *ShowTagKeysMapper) NextChunk() (interface{}, error) {
	log.Println("ShowTagKeysMapper.NextChunk: start")
	defer log.Println("ShowTagKeysMapper.NextChunk: end")
	if m.remote != nil {
		b, err := m.remote.NextChunk()
		if err != nil {
			return nil, err
		} else if b == nil {
			return nil, nil
		}

		names := []string{}
		if err := json.Unmarshal(b.([]byte), &names); err != nil {
			return nil, err
		} else if len(names) == 0 {
			// Mapper on other node sent 0 values so it's done.
			return nil, nil
		}
		return names, nil
	}
	return m.nextChunk()
}

// nextChunk implements next chunk logic for a local shard.
func (m *ShowTagKeysMapper) nextChunk() (interface{}, error) {
	// Allocate array to hold measurement names.
	names := make([]string, 0, m.chunkSize)
	// Get the channel of measurement names from the state.
	measurementNames := m.state.(chan string)
	// Get the next chunk of names.
	for n := range measurementNames {
		names = append(names, n)
		if len(names) == m.chunkSize {
			break
		}
	}
	// See if we've read all the names.
	if len(names) == 0 {
		return nil, nil
	}

	return names, nil
}

// Close closes the mapper.
func (m *ShowTagKeysMapper) Close() {
	if m.remote != nil {
		m.remote.Close()
	}
}
