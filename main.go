package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/flevanti/goini"
)

type schemaObject struct {
	ObjectName              string
	ObjectType              string
	ObjectSignatureOriginal string
	ObjectSignature         string
}

var serversConfigs []serverConfigT
var tempFolderPath string

var objectTypeToFolderMapping = map[string]string{"TABLE": "tables",
	"VIEW":      "views",
	"FUNCTION":  "functions",
	"PROCEDURE": "procedures",
	"SEQUENCE":  "sequences",
	"PIPE":      "pipes"}

const TEMPFOLDERNAMEPREFIX = "snowflake_dumper"
const DDLFOLDER = "ddl"

func main() {
	bootTime := time.Now()
	// TODO MAKE SURE TO GET ERRORS IF USING GO ROUTINES
	var iniConfig = goini.New()
	failOnError(parseIniFile(iniConfig))
	importConfiguration(iniConfig)
	failOnError(initialiseTmpOutputFolder())

	// for the moment leave the temp folder created to grab the file manually
	// defer remoteTempFolder()

	failOnError(dumpEntryPoint())

	// TODO move the files in another location/local git repo?
	// TODO commit changes in the git branch/repo?
	// TODO push changes to git remote...

	fmt.Printf("process completed, it took %s\n", time.Since(bootTime))

}

func remoteTempFolder() {
	fmt.Println("Temp folder removed")
	os.RemoveAll(tempFolderPath)
}

func dumpEntryPoint() error {

	for k := range serversConfigs {
		fmt.Printf("Processing INI section [%s]\n", serversConfigs[k].ConfigName)
		err := serversConfigs[k].dumpIniSection()
		if err != nil {
			return err
		}
	}

	fmt.Println("\n\n\nProcess completed, files are available here:")
	for _, sc := range serversConfigs {
		fmt.Println(sc.DdlFolderFull)
	}
	fmt.Print("\n\n\n\n")
	return nil
}

func initialiseTmpOutputFolder() error {
	var err error
	tempFolderPath, err = ioutil.TempDir("", TEMPFOLDERNAMEPREFIX)
	if err != nil {
		return err
	}
	fmt.Printf("Temp folder for files storage is [%s]\n", tempFolderPath)
	return nil

}

func createSubTempFolder(path string) (string, error) {
	// make sure it is lowercase
	path = strings.ToLower(path)

	path = tempFolderPath + "/" + path

	if checkIfFileExists(path) {
		return "", errors.New(fmt.Sprintf("folder [%s] already present", path))
	}

	return path, os.MkdirAll(path, 0755)
}

func checkIfFileExists(path string) bool {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func parseIniFile(iniConfig *goini.INI) error {

	failOnError(iniConfig.ParseFile("config.ini"))
	iniSectionsCount := len(iniConfig.GetSectionsList())
	fmt.Printf("%d sections found ", iniSectionsCount)

	if iniSectionsCount > 0 {
		fmt.Printf("%v\n", iniConfig.GetSectionsList())
		return nil
	}
	return errors.New("no sections found in the ini file")
}

func importConfiguration(iniConfig *goini.INI) {
	for _, iniSection := range iniConfig.GetSectionsList() {
		fmt.Printf("Importing configuration for section [%s]\n", iniSection)
		importConfigurationSection(iniSection, iniConfig)
	}
}

func importConfigurationSection(iniSection string, ini *goini.INI) {
	var found bool

	c := serverConfigT{}
	c.ConfigName = iniSection

	retrieveIniValue := func(iniKey string, destVar *string) error {
		*destVar, found = ini.SectionGet(iniSection, iniKey)
		if !found {
			return errors.New(fmt.Sprintf("[%s] not found in section [%s]\n", iniKey, iniSection))
		}
		return nil
	}
	retrieveIniValueBool := func(iniKey string, destVar *bool) error {
		*destVar, found = ini.SectionGetBool(iniSection, iniKey)
		if !found {
			return errors.New(fmt.Sprintf("[%s] not found in section [%s]\n", iniKey, iniSection))
		}
		return nil
	}

	failOnError(retrieveIniValue("DBACCOUNT", &c.DbAccount))
	failOnError(retrieveIniValue("DBUSER", &c.DbUser))
	failOnError(retrieveIniValue("DBPASSWORD", &c.DbPassword))
	failOnError(retrieveIniValue("DBUSERROLE", &c.DbUserRole))
	failOnError(retrieveIniValue("DBWAREHOUSE", &c.DbWarehouse))
	failOnError(retrieveIniValue("DBNAME", &c.DbName))
	failOnError(retrieveIniValue("DBSCHEMA", &c.DbSchema))
	failOnError(retrieveIniValueBool("DDLSEPARATEFOLDER", &c.DdlInSeparateFolder))

	if c.DdlInSeparateFolder {
		c.DdlFolder = c.ConfigName + "/" + DDLFOLDER
	} else {
		c.DdlFolder = c.ConfigName
	}

	serversConfigs = append(serversConfigs, c)

}

func getDatabasesList(db *sql.DB) ([]string, error) {
	var dbNames []string
	var dbName string
	var dummy string
	resp, err := db.Query("show databases")
	if err != nil {
		return nil, err
	}
	for resp.Next() {
		err = resp.Scan(&dummy, &dbName, &dummy, &dummy, &dummy, &dummy, &dummy, &dummy, &dummy)
		if err != nil {
			return nil, err
		}
		dbNames = append(dbNames, dbName)
	}

	return dbNames, nil
}

func getSchemasInDatabaseList(db *sql.DB, databaseName string) ([]string, error) {
	var schemaNames []string
	var schemaName string
	var dummy string
	resp, err := db.Query(fmt.Sprintf("show schemas in \"%s\";", databaseName))
	if err != nil {
		return nil, err
	}
	for resp.Next() {
		err = resp.Scan(&dummy, &schemaName, &dummy, &dummy, &dummy, &dummy, &dummy, &dummy, &dummy)
		if err != nil {
			return nil, err
		}
		schemaNames = append(schemaNames, schemaName)
	}

	return schemaNames, nil
}

func retrieveSchemaObjects(sc *serverConfigT, databaseName, schemaName string) ([]schemaObject, error) {
	var l []schemaObject
	var stm string = "select TABLE_NAME as name, '' AS ARGUMENT_SIGNATURE, 'TABLE' as type " +
		"from \"@DB\".\"INFORMATION_SCHEMA\".TABLES " +
		"where TABLE_SCHEMA = '@SCHEMA' and TABLE_TYPE='BASE TABLE'" +
		"UNION ALL " +
		"select TABLE_NAME as name, '' AS ARGUMENT_SIGNATURE, 'VIEW' as type " +
		"from \"@DB\".\"INFORMATION_SCHEMA\".VIEWS " +
		"where TABLE_SCHEMA = '@SCHEMA'" +
		"UNION ALL " +
		"select FUNCTION_NAME, ARGUMENT_SIGNATURE, 'FUNCTION' " +
		"from \"@DB\".\"INFORMATION_SCHEMA\".FUNCTIONS " +
		"where FUNCTION_SCHEMA = '@SCHEMA' " +
		"UNION ALL " +
		"select PROCEDURE_NAME, ARGUMENT_SIGNATURE, 'PROCEDURE' " +
		"from \"@DB\".\"INFORMATION_SCHEMA\".PROCEDURES " +
		"where PROCEDURE_SCHEMA = '@SCHEMA' " +
		"UNION ALL " +
		"select SEQUENCE_NAME, '', 'SEQUENCE' " +
		"from \"@DB\".\"INFORMATION_SCHEMA\".SEQUENCES " +
		"where SEQUENCE_SCHEMA = '@SCHEMA' " +
		"UNION ALL " +
		"select PIPE_NAME, '', 'PIPE' " +
		"from \"@DB\".\"INFORMATION_SCHEMA\".PIPES " +
		"where PIPE_SCHEMA = '@SCHEMA'"
	stm = strings.ReplaceAll(stm, "@DB", databaseName)
	stm = strings.ReplaceAll(stm, "@SCHEMA", schemaName)

	res, err := sc.DbClient.Query(stm)
	if err != nil {
		return nil, err
	}

	// this is for objects that have a signature (need more testing)
	// remove the parameters name
	re1 := regexp.MustCompile("(\\()(.*?)(\\s)")
	re2 := regexp.MustCompile("(, )(.*?)(\\s)")
	for res.Next() {
		so := schemaObject{}
		err = res.Scan(&so.ObjectName, &so.ObjectSignatureOriginal, &so.ObjectType)
		if err != nil {
			return nil, err
		}

		if so.ObjectSignatureOriginal != "" {
			so.ObjectSignature = re1.ReplaceAllString(so.ObjectSignatureOriginal, "(")
			so.ObjectSignature = re2.ReplaceAllString(so.ObjectSignature, ", ")
		}

		l = append(l, so)
	}

	return l, nil
}

func failOnError(err error) {
	if err != nil {

		pc, filename, line, _ := runtime.Caller(1)
		log.Printf("[error] in %s[%s:%d] %v", runtime.FuncForPC(pc).Name(), filename, line, err)
		pc, filename, line, _ = runtime.Caller(2)
		log.Printf("[error] in %s[%s:%d] %v", runtime.FuncForPC(pc).Name(), filename, line, err)
		pc, filename, line, _ = runtime.Caller(3)
		log.Printf("[error] in %s[%s:%d] %v", runtime.FuncForPC(pc).Name(), filename, line, err)
		log.Fatalln("--------------Execution terminated----------------")
	}
}
