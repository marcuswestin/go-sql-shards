package shards

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/marcuswestin/go-cli"
	"github.com/marcuswestin/go-errs"
	"github.com/marcuswestin/go-random"
	"github.com/marcuswestin/go-sql-shards/adapters"
	"github.com/marcuswestin/sqlx"
	"github.com/marcuswestin/sqlx/reflectx"
)

type DB struct {
	*sqlx.DB
}

type ShardSet struct {
	username     string
	password     string
	host         string
	port         int
	dbNamePrefix string
	numShards    int
	maxShards    int
	maxConns     int
	shards       []*DB
	mapper       *reflectx.Mapper
}

func NewShardSet(username string, password string, host string, port int, dbNamePrefix string, numShards int, maxShards int, maxConns int) *ShardSet {
	return &ShardSet{username, password, host, port, dbNamePrefix, numShards, maxShards, maxConns, []*DB{}, nil}
}

func (s *ShardSet) Connect() error {
	return s.connect(true)
}
func (s *ShardSet) MustConnect() {
	if err := s.connect(true); err != nil {
		panic(err)
	}
}
func (s *ShardSet) UseJSONTags() {
	s.mapper = reflectx.NewMapperTagFunc("json", strings.ToUpper, func(value string) string {
		return strings.Split(value, ",")[0]
	})
}

func (s *ShardSet) DropShards() {
	if !cli.YesNo("Drop database from all shards?") {
		os.Exit(-1)
	}
	err := s.connect(false)
	if err != nil {
		panic(err)
	}
	for i, shard := range s.shards {
		cmd := "DROP DATABASE IF EXISTS " + fmt.Sprint(s.dbNamePrefix, i+1)
		log.Println(cmd)
		shard.MustExec(cmd)
	}
}

func (s *ShardSet) SetupShards(schema string) {
	err := s.connect(false)
	if err != nil {
		panic(err)
	}
	for i, shard := range s.shards {
		dbName := fmt.Sprint(s.dbNamePrefix, i+1)
		cmd := "CREATE DATABASE " + dbName
		log.Println(cmd)
		shard.MustExec(cmd)
		shard.MustExec("USE " + dbName)
		statements := strings.Split(string(schema), ";")
		for _, statement := range statements {
			statement = strings.TrimSpace(statement)
			if statement != "" {
				log.Println(statement)
				shard.MustExec(statement)
			}
		}
	}
	return
}

func (s *ShardSet) CreateUser(user string, privileges string, password string) {
	for _, shard := range s.shards {
		cmd := "GRANT " + privileges + " ON *.* TO '" + user + "'@'%' IDENTIFIED BY '" + password + "'"
		log.Println(cmd)
		shard.MustExec(cmd)
		cmd = "GRANT " + privileges + " ON *.* TO '" + user + "'@'localhost' IDENTIFIED BY '" + password + "'"
		log.Println(cmd)
		shard.MustExec(cmd)
	}
}

func (s *ShardSet) Shard(id int64) *DB {
	if id == 0 {
		panic("Bad shard index id 0")
	}
	shardIndex := ((id - 1) % int64(s.maxShards)) // 1->0, 2->1, 3->2 ..., 1000->1000, 1001->0, 1002->1
	return s.shards[shardIndex]
}

func (s *ShardSet) All() []*DB {
	all := make([]*DB, len(s.shards))
	copy(all, s.shards)
	return all
}

func (s *ShardSet) RandomShard() *DB {
	return s.shards[random.Between(0, len(s.shards))]
}

// Internal
///////////

func (s *ShardSet) addShard(i int, selectDb bool) (err error) {
	autoIncrementOffset := i + 1
	dbName := ""
	if selectDb {
		dbName = fmt.Sprint(s.dbNamePrefix, autoIncrementOffset)
	}
	s.shards[i], err = newShard(s, dbName, autoIncrementOffset)
	if err != nil {
		return
	}
	return
}

func newShard(s *ShardSet, dbName string, autoIncrementOffset int) (*DB, error) {
	connVars := adapters.ConnVariables{
		"autocommit":               "true",
		"clientFoundRows":          "true",
		"parseTime":                "true",
		"charset":                  "utf8mb4",
		"collation":                "utf8_unicode_ci",
		"auto_increment_increment": strconv.Itoa(s.maxShards),
		"auto_increment_offset":    strconv.Itoa(autoIncrementOffset),
		"sql_mode":                 "STRICT_ALL_TABLES",
	}

	db, err := adapters.Opener(s.username, s.password, dbName, s.host, s.port, connVars)
	if err != nil {
		return nil, err
	}

	db.Mapper = s.mapper
	db.SetMaxOpenConns(s.maxConns)
	// db.SetMaxIdleConns(n)
	err = db.Ping()
	if err != nil {
		return nil, errs.Wrap(err, nil)
	}
	return &DB{db}, nil
}

func (s *ShardSet) connect(useDb bool) (err error) {
	s.shards = make([]*DB, s.numShards)
	for i := 0; i < s.numShards; i++ {
		err = s.addShard(i, useDb)
		if err != nil {
			return
		}
	}
	return
}
