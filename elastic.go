package lib

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/olivere/elastic/v7"
	log "github.com/sirupsen/logrus"
)

type ElasticClient struct {
	Client           *elastic.Client
	Host             string
	BulkInsertClient *elastic.BulkProcessor
}

type basicAuthTransport struct {
	username string
	password string
}

func (tr *basicAuthTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r.SetBasicAuth(tr.username, tr.password)
	return http.DefaultTransport.RoundTrip(r)
}

func GetElasticClient(urlElastic string, usr string, pwd string) *ElasticClient {

	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	client := &http.Client{
		Transport: &basicAuthTransport{
			username: usr,
			password: pwd,
		},
	}

	elClient, err := elastic.NewClient(
		elastic.SetHttpClient(client),
		elastic.SetURL(urlElastic),
		elastic.SetSniff(false),
		elastic.SetErrorLog(log.New()),
		// elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)),
	)
	if err != nil {
		log.Error("Error ELK: Erro ao conectar no elasticsearch", err)
		return nil
	}

	elCli := ElasticClient{}
	elCli.Host = urlElastic
	elCli.Client = elClient
	return &elCli
}

func (elCli *ElasticClient) CreateIndexIfNotExists(indexName string, settings string) error {

	exists, err := elCli.Client.IndexExists(indexName).Do(context.Background())
	if err != nil {
		return fmt.Errorf("index Exists? %v", err)
	}
	if !exists {
		createIndex, err := elCli.Client.CreateIndex(indexName).
			BodyString(settings).
			Do(context.Background())
		if err != nil {
			return fmt.Errorf("create Index %v", err)
		}
		if !createIndex.Acknowledged {
			return fmt.Errorf("erro ao criar indice - %v", createIndex.Acknowledged)
		} else {
			log.Info("Índice criado: %v", indexName)
		}
	}
	return nil
}

func (elCli *ElasticClient) Close() {
	elCli.Client.Stop()
	if elCli.BulkInsertClient != nil {
		elCli.BulkInsertClient.Close()
	}
}

func (elCli *ElasticClient) InitializeBulkInsert(qtyWorkers int, bufferSize int, bulkSize int, flushInterval time.Duration) error {

	bulkInsert, err := elCli.Client.BulkProcessor().
		Name("bulk-insert-elk").
		Workers(qtyWorkers).
		BulkActions(bufferSize).                    // commit if # requests >= bufferSize
		BulkSize(bulkSize).                         // commit if size of requests >= bulkSize
		FlushInterval(flushInterval * time.Second). // commit every 15s
		Do(context.Background())
	if err != nil {
		return err
	}

	elCli.BulkInsertClient = bulkInsert

	return nil
}

func (elCli *ElasticClient) AddBulkRequest(index string, msg string) {
	r := elastic.NewBulkIndexRequest().Index(index).Doc(msg)
	elCli.BulkInsertClient.Add(r)
}
