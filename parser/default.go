package parser

import (
	"encoding/json"
	"os"
)

type candle struct {
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
	Timestamp int     `json:"datetime"`
}

type fileContents struct {
	Symbol  string   `json:"symbol"`
	Candles []candle `json:"candles"`
}

type DefaultParser struct {
	Contents *fileContents
}

type Parser interface {
	Parse(fname string)
}

func NewDefaultParser() *DefaultParser {
	return &DefaultParser{
		Contents: &fileContents{},
	}
}

func (d *DefaultParser) Parse(fname string) error {
	f, err := os.Open(fname)
	if err != nil {
		return err
	}
	jsoner := json.NewDecoder(f)
	if err = jsoner.Decode(d.Contents); err != nil {
		return err
	}
	return nil
}
