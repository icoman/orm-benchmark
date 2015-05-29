package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/astaxie/beego/orm"
	"github.com/coocood/qbs"
	"github.com/eaigner/hood"
	"github.com/eaigner/jet"
	"github.com/go-xorm/xorm"
	"github.com/jinzhu/gorm"
	"github.com/jmoiron/modl"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"runtime"
	"time"
)

const (
	N            = 10000
	DROP_TABLE   = `DROP TABLE IF EXISTS Benchmark`
	CREATE_TABLE = `CREATE TABLE Benchmark(
			id SERIAL NOT NULL,
			nume character varying(255),
			prenume character varying(255),
			cod bigint,
			cnt integer,
			data timestamp without time zone,
			CONSTRAINT benchmark_pkey PRIMARY KEY (id)
			) WITH (OIDS=FALSE);
	`
)

var (
	user          = flag.String("user", "ioan", "the database user")
	password      = flag.String("password", "***", "the database password")
	port     *int = flag.Int("port", 5432, "the database port")
	server        = flag.String("server", "localhost", "the database server")
	database      = flag.String("database", "benchmark", "the database")

	benchTipe = flag.String("bt", "x", "Benchmark type: sq=sql, sx=sqlx, xo=xorm, go=gorm, ho=hood, qb=qbs, je=jet, beg=beego orm, mo=modl orm")

	connString string
)

type Benchmark struct {
	Id      int `orm:"auto" gorm:"primary_key" xorm:"pk autoincr" qbs:"pk" ` //primary key for beego_orm - gorm - xorm - qbs
	Nume    string
	Prenume string
	Cod     int64
	Cnt     int32
	Data    time.Time
}

type Benchmark1 struct {
	Id      hood.Id `sql:"pk"` //primary key for hood
	Nume    string
	Prenume string
	Cod     int64
	Cnt     int32
	Data    time.Time
}

func TestError(err error) {
	if err != nil {
		panic(err)
	}
}

func test_sql() {
	db, err := sql.Open("postgres", connString)
	TestError(err)
	defer db.Close()

	_, err = db.Exec(DROP_TABLE)
	TestError(err)
	_, err = db.Exec(CREATE_TABLE)
	TestError(err)

	var i int32
	for i = 1; i <= N; i++ {
		var nume = fmt.Sprintf("Test SQL %d", i)
		var prenume = fmt.Sprintf("Test SQL %d", i)
		var q = fmt.Sprintf("INSERT INTO Benchmark (nume,prenume,cod,cnt,data) VALUES('%s','%s',%d,%d,'%s')", nume, prenume, N, i, time.Now().Format(time.ANSIC))
		//fmt.Println("q=", q)
		_, err = db.Exec(q)
		TestError(err)
		if 0 == i%(N/100) {
			fmt.Printf("\rsql   %02d%% ", int(i*100/N))
		}
	}
}

func test_sqlx() {
	db, err := sqlx.Open("postgres", connString)
	TestError(err)
	defer db.Close()

	_, err = db.Exec(DROP_TABLE)
	TestError(err)
	_, err = db.Exec(CREATE_TABLE)
	TestError(err)

	var i int32
	for i = 1; i <= N; i++ {
		var nume = fmt.Sprintf("Test SQLX %d", i)
		var prenume = fmt.Sprintf("Test SQLX %d", i)
		var q = fmt.Sprintf("INSERT INTO Benchmark (nume,prenume,cod,cnt,data) VALUES('%s','%s',%d,%d,'%s')", nume, prenume, N, i, time.Now().Format(time.ANSIC))
		_, err = db.Exec(q)
		TestError(err)
		if 0 == i%(N/100) {
			fmt.Printf("\rsqlx  %02d%% ", int(i*100/N))
		}
	}
}

func test_xorm() {
	db, err := xorm.NewEngine("postgres", connString)
	TestError(err)
	defer db.Close()
	db.ShowSQL = false

	err = db.DropTables(&Benchmark{})
	TestError(err)
	err = db.CreateTables(&Benchmark{})
	TestError(err)

	var i int32
	for i = 1; i <= N; i++ {
		var nume = fmt.Sprintf("Test xorm %d", i)
		var prenume = fmt.Sprintf("Test xorm %d", i)
		_, err := db.Insert(&Benchmark{Nume: nume, Prenume: prenume, Cod: N, Cnt: i, Data: time.Now()})
		TestError(err)
		if 0 == i%(N/100) {
			fmt.Printf("\rxorm  %02d%% ", int(i*100/N))
		}
	}
}

func test_gorm() {
	db, err := gorm.Open("postgres", connString)
	TestError(err)
	defer db.Close()

	// Disable table name's pluralization
	db.SingularTable(true)
	// Drop table - merge si da avertisment daca nu exista tabela
	db.DropTable(&Benchmark{})
	// Create table
	db.CreateTable(&Benchmark{})

	var i int32
	for i = 1; i <= N; i++ {
		var nume = fmt.Sprintf("Test gorm %d", i)
		var prenume = fmt.Sprintf("Test gorm %d", i)
		db.Create(&Benchmark{Nume: nume, Prenume: prenume, Cod: N, Cnt: i, Data: time.Now()})
		if 0 == i%(N/100) {
			fmt.Printf("\rgorm  %02d%% ", int(i*100/N))
		}
	}
}

func test_hood() {
	db, err := hood.Open("postgres", connString)
	TestError(err)

	tx := db.Begin()
	err = tx.DropTableIfExists(&Benchmark1{})
	TestError(err)
	err = tx.CreateTable(&Benchmark1{})
	TestError(err)

	var i int32
	for i = 1; i <= N; i++ {
		var nume = fmt.Sprintf("Test hood %d", i)
		var prenume = fmt.Sprintf("Test hood %d", i)
		_, err = tx.Save(&Benchmark1{Nume: nume, Prenume: prenume, Cod: N, Cnt: i, Data: time.Now()})
		TestError(err)
		if 0 == i%(N/100) {
			fmt.Printf("\rhood  %02d%% ", int(i*100/N))
		}
	}
	err = tx.Commit()
	TestError(err)
}

func test_qbs() {
	//not working - panic: unexpected command tag INSERT
	qbs.Register("postgres", connString, "qbs_test", qbs.NewPostgres())
	migration, err := qbs.GetMigration()
	TestError(err)
	defer migration.Close()
	migration.DropTable(new(Benchmark))
	err = migration.CreateTableIfNotExists(new(Benchmark))
	TestError(err)

	var i int32
	for i = 1; i <= N; i++ {
		var q *qbs.Qbs
		q, err = qbs.GetQbs()
		TestError(err)
		defer q.Close()
		//q.Log = true

		err = q.Begin()
		TestError(err)
		var nume = fmt.Sprintf("Test qbs %d", i)
		var prenume = fmt.Sprintf("Test qbs %d", i)
		_, err = q.Save(&Benchmark{Nume: nume, Prenume: prenume, Cod: N, Cnt: i, Data: time.Now()})
		TestError(err)
		if 0 == i%(N/100) {
			fmt.Printf("\rqbs   %02d%% ", int(i*100/N))
		}
		err = q.Commit()
		TestError(err)
	}
}

func test_jet() {
	db, err := jet.Open("postgres", connString)
	TestError(err)
	defer db.Close()

	err = db.Query(DROP_TABLE).Run()
	TestError(err)
	err = db.Query(CREATE_TABLE).Run()
	TestError(err)

	var i int32
	for i = 1; i <= N; i++ {
		var nume = fmt.Sprintf("Test jet %d", i)
		var prenume = fmt.Sprintf("Test jet %d", i)
		err = db.Query("INSERT INTO Benchmark (nume, prenume, cod, cnt, data) VALUES ($1,$2,$3,$4,$5)", nume, prenume, N, i, string(time.Now().Format(time.ANSIC))).Run()
		TestError(err)
		if 0 == i%(N/100) {
			fmt.Printf("\rjet   %02d%% ", int(i*100/N))
		}
	}
}

func test_beego_orm() {
	//Beedb is being deprecated in favor of Beego.orm
	var err error
	orm.RegisterDriver("postgres", orm.DR_Postgres)
	orm.RegisterDataBase("default", "postgres", connString)
	orm.RegisterModel(new(Benchmark))
	db := orm.NewOrm()
	db.Using("default")

	db.Raw(DROP_TABLE).Exec()
	//TestError(err)
	db.Raw(CREATE_TABLE).Exec()
	//TestError(err)
	db.Commit()

	var i int32
	for i = 1; i <= N; i++ {
		var nume = fmt.Sprintf("Test beego %d", i)
		var prenume = fmt.Sprintf("Test beego %d", i)
		_, err = db.Insert(&Benchmark{Nume: nume, Prenume: prenume, Cod: N, Cnt: i, Data: time.Now()})
		TestError(err)
		if 0 == i%(N/100) {
			fmt.Printf("\rbeego %02d%% ", int(i*100/N))
		}
	}
	db.Commit()
}

func test_modl() {
	db, err := sql.Open("postgres", connString)
	TestError(err)
	defer db.Close()
	dbmap := modl.NewDbMap(db, modl.PostgresDialect{})

	_, err = db.Exec(DROP_TABLE)
	TestError(err)
	_, err = db.Exec(CREATE_TABLE)
	TestError(err)

	dbmap.AddTable(Benchmark{}, "Benchmark").SetKeys(true, "id")

	tx, _ := dbmap.Begin()
	var i int32
	for i = 1; i <= N; i++ {

		var nume = fmt.Sprintf("Test modl %d", i)
		var prenume = fmt.Sprintf("Test modl %d", i)
		err = tx.Insert(&Benchmark{Nume: nume, Prenume: prenume, Cod: N, Cnt: i, Data: time.Now()})
		TestError(err)
		if 0 == i%(N/100) {
			fmt.Printf("\rmodl  %02d%% ", int(i*100/N))
		}
	}
	tx.Commit()
}

func bench_test(f func()) {
	var start = time.Now().UnixNano()
	f()
	var end = time.Now().UnixNano()
	durata := float64(end-start) / 1e9
	fmt.Printf("- done in %.2f sec -> %.1f val/s\n", durata, float64(N/durata))
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(1 * runtime.NumCPU())
	connString = fmt.Sprintf("host=%s user=%s password=%s port=%d dbname=%s sslmode=disable", *server, *user, *password, *port, *database)
	fmt.Printf("Test benchmark POSTGRES insert %d values\n", N)

	switch {

	case *benchTipe == "all":
		bench_test(test_sql)
		bench_test(test_sqlx)
		bench_test(test_xorm)
		bench_test(test_gorm)
		bench_test(test_hood)
		bench_test(test_jet)
		//bench_test(test_qbs) //panic: unexpected command tag INSERT
		bench_test(test_modl)
		bench_test(test_beego_orm)

	case *benchTipe == "sq":
		bench_test(test_sql)

	case *benchTipe == "sx":
		bench_test(test_sqlx)

	case *benchTipe == "xo":
		bench_test(test_xorm)

	case *benchTipe == "go":
		bench_test(test_gorm)

	case *benchTipe == "ho":
		bench_test(test_hood)

	case *benchTipe == "je":
		bench_test(test_jet)

	case *benchTipe == "qb":
		bench_test(test_qbs) //panic: unexpected command tag INSERT

	case *benchTipe == "mo":
		bench_test(test_modl)

	case *benchTipe == "beg":
		bench_test(test_beego_orm)

	default:
		fmt.Println("Nothing selected.")
	}
}
