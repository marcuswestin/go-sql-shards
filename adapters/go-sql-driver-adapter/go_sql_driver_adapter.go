package go_sql_driver_adapter

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/marcuswestin/go-errs"
	"github.com/marcuswestin/go-sql-shards/adapters"
	"github.com/marcuswestin/sqlx"
)

func init() {
	adapters.SetOpener(goSqlDriverOpener)
}

func goSqlDriverOpener(username, password, dbName, host string, port int, connVars adapters.ConnVariables) (*sqlx.DB, error) {
	sourceString := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?%s",
		username, password, host, port, dbName, connVars.Join("&"))
	db, err := sqlx.Connect("mysql", sourceString)
	if err != nil {
		return nil, errs.Wrap(err, errs.Info{})
	}
	return db, nil
}
