package main

import (
	"errors"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	BigQuery []BigQuery `yaml:"biqguery"`
}

type BigQuery struct {
	Key     string `yaml:"key"`
	Dataset string `yaml:"dataset"`
	Table   string `yaml:"table"`
	Suffix  string `yaml:"suffix"`
}

func LoadConfig(path string) Config {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	//fmt.Printf("buf: %+v", string(buf))

	var c Config
	err = yaml.Unmarshal(buf, &c)
	if err != nil {
		panic(err)
	}
	//fmt.Printf("c: %+v", c)
	return c
}

func (c Config) GetBqConfig(key string) (*BigQuery, error) {
	var bqConfig *BigQuery
	for _, v := range c.BigQuery {
		if key == v.Key {
			bqConfig = &v
			break
		}
	}
	if bqConfig == nil {
		return nil, errors.New("can not find config")
	}
	return bqConfig, nil
}
