package main

import (
	"context"
	"io/ioutil"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
)

const (
	projectID = "your-project"
	dataset   = "sample_dataset"
	table1    = "schema_sample_table_01"
	table2    = "schema_sample_table_02"
	table3    = "schema_sample_table_03"
)

/*
 * 1. bigquery.Schema: field個別にSchema指定
 * ref: https://cloud.google.com/bigquery/docs/schemas#go
 */
func createTableBySchema1() error {
	// client
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return err
	}

	// schema
	schema := bigquery.Schema{
		{Name: "id", Required: true, Type: bigquery.StringFieldType},
		{Name: "data", Required: false, Type: bigquery.StringFieldType},
		{Name: "timestamp", Required: false, Type: bigquery.TimestampFieldType},
	}
	metaData := &bigquery.TableMetadata{Schema: schema}

	// create table
	t := client.Dataset(dataset).Table(table1)
	if err := t.Create(ctx, metaData); err != nil {
		return err
	}

	log.Printf("create table: %+v:%+v.%+v", projectID, dataset, table1)
	return nil
}

/*
 * 2. bigquery.InferSchema: struct + タグでSchema指定してみる
 * ref: https://pkg.go.dev/cloud.google.com/go/bigquery#InferSchema
 */
type Item struct {
	ID        string    `bigquery:"id" mode:"nullable"`
	Data      string    `bigquery:"data" mode:"nullable"`
	Timestamp time.Time `bigquery:"timestamp" mode:"nullable"`
}

func createTableBySchema2() error {
	// client
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return err
	}

	// schema
	schema, err := bigquery.InferSchema(Item{})
	metaData := &bigquery.TableMetadata{Schema: schema}

	// create table
	t := client.Dataset(dataset).Table(table2)
	if err := t.Create(ctx, metaData); err != nil {
		return err
	}

	log.Printf("create table: %+v:%+v.%+v", projectID, dataset, table2)
	return nil
}

/*
 * 3. bigquery.SchemaFromJSON: jsonファイルでSchema指定
 * ref: https://pkg.go.dev/cloud.google.com/go/bigquery#SchemaFromJSON
 */
func createTableBySchema3() error {
	// client
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return err
	}

	// schema
	path := "./schema/schemaSampleTable3.json"
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	schema, err := bigquery.SchemaFromJSON(buf)
	metaData := &bigquery.TableMetadata{Schema: schema}

	// create table
	t := client.Dataset(dataset).Table(table3)
	if err := t.Create(ctx, metaData); err != nil {
		return err
	}

	log.Printf("create table: %+v:%+v.%+v", projectID, dataset, table3)
	return nil
}

func main() {
	if err := createTableBySchema1(); err != nil {
		log.Printf("err: %+v", err)
	}
	if err := createTableBySchema2(); err != nil {
		log.Printf("err: %+v", err)
	}
	if err := createTableBySchema3(); err != nil {
		log.Printf("err: %+v", err)
	}
}
