package point

import (
	"bufio"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/influxdata/influx-stress/lineprotocol"
)

// The point struct implements the lineprotocol.Point interface.
type point struct {
	seriesKey []byte

	// Note here that Ints and Floats are exported so they can be modified outside
	// of the point struct
	Ints   []*lineprotocol.Int
	Floats []*lineprotocol.Float

	// The fields slice should contain exactly Ints and Floats. Having this
	// slice allows us to avoid iterating through Ints and Floats in the Fields
	// function.
	fields []lineprotocol.Field

	time *lineprotocol.Timestamp
}

// New returns a new point without setting the time field.
func New(sk []byte, ints []string, floats []string, p lineprotocol.Precision) *point {
	fields := []lineprotocol.Field{}
	e := &point{
		seriesKey: sk,
		time:      lineprotocol.NewTimestamp(p),
		fields:    fields,
	}

	for _, i := range ints {
		n := &lineprotocol.Int{Key: []byte(i)}
		e.Ints = append(e.Ints, n)
		e.fields = append(e.fields, n)
	}

	for _, f := range floats {
		n := &lineprotocol.Float{Key: []byte(f)}
		e.Floats = append(e.Floats, n)
		e.fields = append(e.fields, n)
	}

	return e
}

// Series returns the series key for a point.
func (p *point) Series() []byte {
	return p.seriesKey
}

// Fields returns the fields for a a point.
func (p *point) Fields() []lineprotocol.Field {
	return p.fields
}

// Time returns the timestamps for a point.
func (p *point) Time() *lineprotocol.Timestamp {
	return p.time
}

// SetTime set the t to be the timestamp for a point.
func (p *point) SetTime(t time.Time) {
	p.time.SetTime(&t)
}

// Update increments the value of all of the Int and Float
// fields by 1.
func (p *point) Update() {
	for _, i := range p.Ints {
		atomic.AddInt64(&i.Value, int64(1))
	}

	for _, f := range p.Floats {
		// Need to do something else here
		// There will be a race here
		f.Value += 1.0
	}
}

// NewPoints returns a slice of Points of length seriesN shaped like the given seriesKey.
func NewPoints(seriesKey, fields string, seriesN int, pc lineprotocol.Precision) []lineprotocol.Point {
	pts := []lineprotocol.Point{}
	series := generateSeriesKeys(seriesKey, seriesN)
	ints, floats := generateFieldSet(fields)
	//ints := [1]string{"type"}
	for _, sk := range series {
		p := New(sk, ints, floats, pc)
		pts = append(pts, p)
	}

	return pts
}

func loadFieldsMap(fieldsPath string) (map[string]string, error) {
	file, err := os.Open(fieldsPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fields := map[string]string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.SplitN(scanner.Text(), " ", 2)
		fields[line[0]] = line[1]
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return fields, nil
}

func NewPointsFromPath(seriesKeyPath, fieldsPath string, pc lineprotocol.Precision) []lineprotocol.Point {
	file, err := os.Open(seriesKeyPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var series [][]byte
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		series = append(series, []byte(scanner.Text()))
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fields, err := loadFieldsMap(fieldsPath)
	if err != nil {
		log.Fatal(err)
	}

	pts := []lineprotocol.Point{}
	for _, sk := range series {
		ints, floats := generateFieldSet(fields[strings.SplitN(string(sk), ",", 2)[0]])
		p := New(sk, ints, floats, pc)
		pts = append(pts, p)
	}

	return pts
}
