package lib

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	log "github.com/sirupsen/logrus"
)

type MSSQLClient struct {
	SQLConfigJSON  string
	SQLConns       map[string]*sql.DB
	SQLDwConns     map[string]*sql.DB
	SQLCentralConn *sql.DB
	SQLDatabases   []string
	SQLDws         []string
	DBAbbrevs      map[string]string
	DBTimezones    map[string]string
}

type SQLConfig struct {
	Central   []SQLConfigItem `json:"central"`
	Databases []SQLConfigItem `json:"dbs"`
	Dws       []SQLConfigItem `json:"dws"`
}

type SQLConfigItem struct {
	Index            string `json:"idx"`
	Name             string `json:"name"`
	ConnectionString string `json:"conn"`
	Timezone         string `json:"timezone"`
}

func GetMSSQLClient(sqlConfigJSON string) *MSSQLClient {
	client := MSSQLClient{}
	client.SQLConfigJSON = sqlConfigJSON
	client.connect()
	return &client
}

func (client *MSSQLClient) connect() {

	sqlConfig := SQLConfig{}
	err := json.Unmarshal([]byte(client.SQLConfigJSON), &sqlConfig)
	if err != nil {
		log.Error("Error Lendo JSON config", err)
	}

	client.SQLDatabases = []string{}
	client.SQLDws = []string{}
	client.SQLConns = make(map[string]*sql.DB)
	client.DBAbbrevs = make(map[string]string)
	client.DBTimezones = make(map[string]string)

	db := sqlConfig.Central[0]
	conn, err := connectToMSSQLServer(db.ConnectionString)
	if err != nil {
		log.Error("SQL Connection failed:", err.Error())
	}
	conn.SetConnMaxLifetime(1 * time.Hour)
	client.SQLCentralConn = conn

	for _, db := range sqlConfig.Databases {
		client.SQLDatabases = append(client.SQLDatabases, db.Name)
		client.DBAbbrevs[db.Name] = db.Index
		client.DBTimezones[db.Name] = db.Timezone

		conn, err := connectToMSSQLServer(db.ConnectionString)
		if err != nil {
			log.Error("SQL Connection failed:", err.Error())
		}
		conn.SetConnMaxLifetime(1 * time.Hour)
		client.SQLConns[db.Name] = conn
	}

	for _, db := range sqlConfig.Dws {
		client.SQLDws = append(client.SQLDws, db.Name)
		client.DBAbbrevs[db.Name] = db.Index
		client.DBTimezones[db.Name] = db.Timezone

		conn, err := connectToMSSQLServer(db.ConnectionString)
		if err != nil {
			log.Error("SQL Connection failed:", err.Error())
		}
		conn.SetConnMaxLifetime(1 * time.Hour)
		client.SQLDwConns[db.Name] = conn
	}

	log.Info("MSSQL ok.", client.DBAbbrevs)
}

func connectToMSSQLServer(connString string) (*sql.DB, error) {
	// connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s", host, user, pwd, port, db)

	conn, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, fmt.Errorf("open connection failed: %v", err)
	}

	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("open connection ping failed: %v", err)
	}

	return conn, nil
}

func (client *MSSQLClient) Close() {
	for _, db := range client.SQLConns {
		db.Close()
	}
	client.SQLCentralConn.Close()
}

func (client *MSSQLClient) GetUTCTimeForSQL(dt string) string {
	return FormatDateWithoutTZ(dt, "UTC")
}

func (client *MSSQLClient) GetLocalTimeForSQL(dt string, db string) string {
	return FormatDateWithoutTZ(dt, client.DBTimezones[db])
}
