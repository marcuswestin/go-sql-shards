package myysql_adapter

import (
	"github.com/marcuswestin/go-errs"
	"github.com/marcuswestin/go-sql-shards/adapters"
	"github.com/marcuswestin/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

func init() {
	adapters.SetOpener(mymysqlDriverOpener)
}

func mymysqlDriverOpener(username, password, dbName, host string, port int, connVars adapters.ConnVariables) (*sqlx.DB, error) {
	db, stdErr := sqlx.Connect("sqlite3", dbName)
	if stdErr != nil {
		return nil, errs.Wrap(stdErr, errs.Info{})
	}
	return db, nil
}
