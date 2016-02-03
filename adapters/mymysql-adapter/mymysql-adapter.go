package myysql_adapter

import (
	"fmt"

	_ "github.com/asappinc/mymysql/godrv"
	"github.com/marcuswestin/go-errs"
	"github.com/marcuswestin/go-sql-shards/adapters"
	"github.com/marcuswestin/sqlx"
)

func init() {
	adapters.SetOpener(mymysqlDriverOpener)
}

func mymysqlDriverOpener(username, password, dbName, host string, port int, connVars adapters.ConnVariables) (*sqlx.DB, error) {
	sourceString := fmt.Sprintf(
		"tcp:%s:%d,%s*%s/%s/%s",
		host, port, connVars.Join(","), dbName, username, password)
	db, stdErr := sqlx.Connect("mymysql", sourceString)
	if stdErr != nil {
		return nil, errs.Wrap(stdErr, errs.Info{})
	}
	return db, nil
}
