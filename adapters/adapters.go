package adapters

import (
	"strings"

	"github.com/marcuswestin/sqlx"
)

// Opener
/////////

var Opener DBOpener

type DBOpener func(username, password, dbName, host string, port int, connVars ConnVariables) (*sqlx.DB, error)

func SetOpener(opener DBOpener) {
	if Opener != nil {
		panic("DBOpener already set - did you import two driver adapters?")
	}
	Opener = opener
}

// Connection variables
///////////////////////

type ConnVariables map[string]string

func (connVars ConnVariables) Join(sep string) string {
	kvps := make([]string, len(connVars))
	i := 0
	for param, val := range connVars {
		kvps[i] = param + "=" + val
		i += 1
	}
	return strings.Join(kvps, sep)
}
