package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	sf "github.com/snowflakedb/gosnowflake"
)

type serverConfigT struct {
	ConfigName          string
	DbAccount           string
	DbUser              string
	DbPassword          string
	DbUserRole          string
	DbWarehouse         string
	DbName              string
	DbSchema            string
	OutputFolder        string
	DdlInSeparateFolder bool
	DdlFolder           string
	DdlFolderFull       string
	DbClient            *sql.DB
}

func (sc *serverConfigT) connect() error {
	cfg := &sf.Config{
		Account:   sc.DbAccount,
		User:      sc.DbUser,
		Password:  sc.DbPassword,
		Role:      sc.DbUserRole,
		Warehouse: sc.DbWarehouse,
		Schema:    sc.DbSchema,
		Database:  sc.DbName,
	}

	dsn, err := sf.DSN(cfg)
	if err != nil {
		return err
	}

	sc.DbClient, err = sql.Open("snowflake", dsn)
	if err != nil {
		return err
	}

	return nil
}

func (sc *serverConfigT) disconnect() error {
	return sc.DbClient.Close()
}

func (sc *serverConfigT) dumpIniSection() error {
	var l []string
	var wg sync.WaitGroup

	ddlFolderFull, err := createSubTempFolder(sc.DdlFolder)
	if err != nil {
		return err
	}
	sc.DdlFolderFull = ddlFolderFull

	err = sc.connect()
	if err != nil {
		return err
	}
	defer sc.disconnect()

	l, err = getDatabasesList(sc.DbClient)
	if err != nil {
		return err
	}

	for _, databaseName := range l {
		fmt.Printf("%s\n", databaseName)
		wg.Add(1)
		go sc.dumpDatabase(databaseName, &wg)
	}

	wg.Wait()

	return nil
}

func (sc *serverConfigT) dumpDatabase(databaseName string, wg *sync.WaitGroup) {
	defer wg.Done()
	_, err := createSubTempFolder(sc.DdlFolder + "/" + databaseName)
	failOnError(err)

	var l []string
	l, err = getSchemasInDatabaseList(sc.DbClient, databaseName)
	failOnError(err)

	fmt.Printf("%v\n", l)
	for _, schemaName := range l {
		if schemaName == "INFORMATION_SCHEMA" {
			continue
		}
		wg.Add(1)
		go sc.dumpSchema(databaseName, schemaName, wg)
	}

}

func (sc *serverConfigT) dumpSchema(databaseName, schemaName string, wg *sync.WaitGroup) {
	defer wg.Done()
	_, err := createSubTempFolder(sc.DdlFolder + "/" + databaseName + "/" + schemaName)
	failOnError(err)
	var l []schemaObject
	l, err = retrieveSchemaObjects(sc, databaseName, schemaName)
	failOnError(err)

	fmt.Printf("[%s].[%s] %d objects found\n", databaseName, schemaName, len(l))
	for _, obj := range l {
		fmt.Printf("Getting ddl for [%s].[%s].[%s] (%s)\n", databaseName, schemaName, obj.ObjectName, obj.ObjectType)
		ddl, err := sc.getDdl(databaseName, schemaName, &obj)
		failOnError(err)
		failOnError(sc.saveDdl(ddl, databaseName, schemaName, &obj))
	}
}

func (sc *serverConfigT) getDdl(databaseName, schemaName string, obj *schemaObject) (string, error) {
	var ddl string

	stm := fmt.Sprintf("select get_ddl('%s', '\"%s\".\"%s\".\"%s\"%s', true)", obj.ObjectType, databaseName, schemaName, obj.ObjectName, obj.ObjectSignature)
	res, err := sc.DbClient.Query(stm, obj.ObjectType, databaseName, schemaName, obj.ObjectName)
	if err != nil {
		return "", err
	}
	res.Next()
	err = res.Scan(&ddl)
	if err != nil {
		return "", err
	}
	return ddl, nil
}

func (sc *serverConfigT) saveDdl(ddl, databaseName, schemaName string, obj *schemaObject) error {
	filename := obj.ObjectName
	if obj.ObjectType == "FUNCTION" {
		filename = obj.ObjectName + obj.ObjectSignature
	}

	objectTypeFolder := objectTypeToFolderMapping[obj.ObjectType]

	_, _ = createSubTempFolder(sc.DdlFolder + "/" + databaseName + "/" + schemaName + "/" + objectTypeFolder)

	body := []byte(ddl)
	filePath := strings.ToLower(fmt.Sprintf("%s/%s/%s/%s/%s/%s.sql", tempFolderPath, sc.DdlFolder, databaseName, schemaName, objectTypeFolder, filename))
	if checkIfFileExists(filePath) {
		filePath = strings.ToLower(fmt.Sprintf("%s/%s/%s/%s/%s/%s_duplicate%d.sql", tempFolderPath, sc.DdlFolder, databaseName, schemaName, objectTypeFolder, filename, time.Now().Unix()))
	}
	err := os.WriteFile(filePath, body, 0644)
	if err != nil {
		return err
	}
	return nil
}
