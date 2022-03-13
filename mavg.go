package main

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type avgState struct {
	sum float64
	cnt uint
}

type movingAvg struct {
	windowHead time.Time
	windowTail time.Time
	end        time.Time
	byMinute   map[time.Time]avgState
	state      avgState
}

func calculateAvg(reader io.Reader, writer io.Writer, wsize uint) error {
	ma, err := readAggregated(reader, wsize)
	if err != nil {
		return err
	}

	for ma.windowHead.Before(ma.end) {
		ma.writeAvg(writer)
		ma.advanceHead()
		ma.advanceTail()
	}
	return nil
}

func (ma *movingAvg) writeAvg(writer io.Writer) error {
	var avg float64
	if ma.state.cnt != 0 {
		avg = ma.state.sum / float64(ma.state.cnt)
	}
	data, err := sjson.SetBytes([]byte{}, "date", ma.windowHead.Add(time.Minute).Format("2006-01-02 15:04:00"))
	if err != nil {
		return err
	}
	data, err = sjson.SetBytes(data, "average_delivery_time", avg)
	if err != nil {
		return err
	}
	_, err = writer.Write(data)
	writer.Write([]byte("\n"))
	return err
}

func (ma *movingAvg) advanceHead() {
	ma.windowHead = ma.windowHead.Add(time.Minute)
	if v, ok := ma.byMinute[ma.windowHead]; ok {
		ma.state.sum += v.sum
		ma.state.cnt += v.cnt
	}
}

func (ma *movingAvg) advanceTail() {
	if v, ok := ma.byMinute[ma.windowTail]; ok {
		ma.state.sum -= v.sum
		ma.state.cnt -= v.cnt
	}
	ma.windowTail = ma.windowTail.Add(time.Minute)
}

func readAggregated(reader io.Reader, wsize uint) (*movingAvg, error) {
	ma := movingAvg{
		byMinute: make(map[time.Time]avgState),
	}
	sc := bufio.NewScanner(reader)
	for sc.Scan() {
		data := sc.Bytes()
		ts, err := getTimestamp(data)
		if err != nil {
			return nil, err
		}
		if ma.windowHead.IsZero() {
			ma.windowHead = ts.Add(-time.Minute)
			ma.windowTail = ts.Add(-time.Duration(wsize) * time.Minute)
		}
		ma.end = ts
		dur, err := getDuration(data)
		if err != nil {
			return nil, err
		}
		v := ma.byMinute[ts]
		v.sum += dur
		v.cnt += 1
		ma.byMinute[ts] = v
	}
	ma.end = ma.end.Add(time.Minute)
	return &ma, sc.Err()
}

func getTimestamp(data []byte) (time.Time, error) {
	tsRes := gjson.GetBytes(data, "timestamp")
	if !tsRes.Exists() {
		return time.Time{}, fmt.Errorf("no timestamp in %s", data)
	}
	ts, err := time.Parse("2006-01-02 15:04:05.000000", tsRes.String())
	if err != nil {
		return time.Time{}, err
	}
	ts = ts.Round(time.Minute)
	return ts, nil
}

func getDuration(data []byte) (float64, error) {
	durRes := gjson.GetBytes(data, "duration")
	if !durRes.Exists() {
		return 0, fmt.Errorf("no duration in %s", data)
	}
	return durRes.Float(), nil
}
