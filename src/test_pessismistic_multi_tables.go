package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"modernc.org/mathutil"
)

var (
	addr         = flag.String("addr", "127.0.0.1:4000", "tidb address")
	concurrency  = flag.Int("concurrency", 32, "concurrency")
	loadData     = flag.Bool("load-data", false, "load data before run")
	tableSize    = flag.Uint64("table-size", 400000, "table size")
	ignoreO      = flag.String("ignore-o", "9007,1105", "ignored error code for optimistic transaction, separated by comma")
	ignoreP      = flag.String("ignore-p", "1213", "ignored error code for pessimistic transaction, separated by comma")
	mode         = flag.String("mode", "mix", "transaction mode, mix|pessimistic|optimistic")
	tables       = flag.Int("tables", 4, "number of test tables")
	insertDelete = flag.Bool("insert-delete", false, "run insert delete transactions")
	ignoreCodesO []int
	ignoreCodesP []int
	successTxn   uint64
	failTxn      uint64
)

const numPartitions = 4

func main() {
	flag.Parse()
	parts := strings.Split(*ignoreP, ",")
	for _, part := range parts {
		iv, _ := strconv.Atoi(part)
		ignoreCodesP = append(ignoreCodesP, iv)
	}
	parts = strings.Split(*ignoreO, ",")
	for _, part := range parts {
		iv, _ := strconv.Atoi(part)
		ignoreCodesO = append(ignoreCodesO, iv)
	}

	db, err := sql.Open("mysql", "root:@tcp("+*addr+")/test")
	if err != nil {
		log.Fatal(err)
	}
	if *loadData {
		err = LoadData(db, *tableSize)
		if err != nil {
			log.Fatal(err)
		}
		return
	}
	wg := new(sync.WaitGroup)
	wg.Add(*concurrency)
	for i := 0; i < *concurrency; i++ {
		se, err := NewSession(db, uint64(i), *tableSize, numPartitions)
		if err != nil {
			log.Fatal(err)
		}
		go se.Run(wg)
	}
	if !*insertDelete {
		go checkLoop(db)
	}
	go statsLoop()
	wg.Wait()
}

const batchSize = 100

func LoadData(db *sql.DB, maxSize uint64) error {
	for i := 0; i < *tables; i++ {
		tableName := fmt.Sprintf("t%d", i)
		log.Printf("loading table: %s\n", tableName)
		if _, err := db.Exec(fmt.Sprintf("drop table if exists %s", tableName)); err != nil {
			return nil
		}
		createTableStmt := fmt.Sprintf("create table %s ("+
			"id bigint primary key,"+
			"u bigint unsigned unique,"+
			"i bigint, c bigint,"+
			"index i (i))", tableName)
		if _, err := db.Exec(createTableStmt); err != nil {
			return err
		}
		for i := uint64(0); i < maxSize; i += batchSize {
			if _, err := db.Exec(insertSql(tableName, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func insertSql(tableName string, beginID uint64) string {
	var values []string
	for i := beginID; i < beginID+batchSize; i++ {
		value := fmt.Sprintf("(%d, %d, %d, %d)", i, i, i, 0)
		values = append(values, value)
	}
	return fmt.Sprintf("insert %s values %s", tableName, strings.Join(values, ","))
}

type Session struct {
	seID          uint64
	isPessimistic bool
	conn          *sql.Conn
	stmts         []func(ctx context.Context) error
	ran           *randIDGenerator
	addedCount    int
	txnStart      time.Time
	commitStart   time.Time
}

func NewSession(db *sql.DB, seID, maxSize uint64, numPartitions uint64) (*Session, error) {
	ctx := context.Background()
	con, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	se := &Session{
		seID: seID,
		conn: con,
		ran:  newRandIDGenerator(maxSize, numPartitions),
	}
	switch *mode {
	case "pessimistic":
		se.isPessimistic = true
	case "mix":
		se.isPessimistic = se.seID%2 == 1
	}
	se.stmts = append(se.stmts,
		se.updateIndex,
		se.updateIndexRange,
		se.updateRange,
		se.updateUniqueIndex,
		se.replace,
		se.deleteInsert,
		se.selectForUpdate,
		se.plainSelect,
	)
	return se, nil
}

func (se *Session) runTransaction(parent context.Context) error {
	se.reset()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	beginSQL := "begin /*!90000 optimistic */"
	if se.isPessimistic {
		beginSQL = "begin /*!90000 pessimistic */"
	}
	_, err := se.conn.ExecContext(ctx, beginSQL)
	if err != nil {
		return err
	}
	numStmts := 1 + se.ran.uniform.Intn(5)
	for i := 0; i < numStmts; i++ {
		stmtType := se.ran.uniform.Intn(len(se.stmts))
		f := se.stmts[stmtType]
		err = f(ctx)
		if err != nil {
			se.handleError(ctx, err, false)
			return nil
		}
	}
	se.commitStart = time.Now()
	_, err = se.conn.ExecContext(ctx, "commit")
	if err != nil {
		se.handleError(ctx, err, true)
	} else {
		atomic.AddUint64(&successTxn, 1)
	}
	return nil
}

func getErrorCode(err error) int {
	var code int
	_, err1 := fmt.Sscanf(err.Error(), "Error %d:", &code)
	if err1 != nil {
		return -1
	}
	return code
}

func (se *Session) handleError(ctx context.Context, err error, isCommit bool) {
	atomic.AddUint64(&failTxn, 1)
	_, _ = se.conn.ExecContext(ctx, "rollback")
	code := getErrorCode(err)
	ignoreCodes := ignoreCodesO
	if se.isPessimistic {
		ignoreCodes = ignoreCodesP
	}
	for _, ignoreCode := range ignoreCodes {
		if ignoreCode == code {
			return
		}
	}
	txnMode := "optimistic"
	if se.isPessimistic {
		txnMode = "pessimistic"
	}
	if isCommit {
		log.Println(txnMode, "txnDur", time.Since(se.txnStart), "commitDur", time.Since(se.commitStart), err)
	} else {
		log.Println(txnMode, se.isPessimistic, "txnDur", time.Since(se.txnStart), err)
	}
}

func (se *Session) Run(wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()
	runFunc := se.runTransaction
	for {
		if err := runFunc(ctx); err != nil {
			log.Println("begin error", err)
			return
		}
	}
}

func (se *Session) deleteInsert(ctx context.Context) error {
	return nil
	// TODO: add deleteInsert for multiple tables
	//rowID := se.ran.nextRowID()
	//row := se.conn.QueryRowContext(ctx, fmt.Sprintf("select c from t where id = %d for update", rowID))
	//var cnt int64
	//err := row.Scan(&cnt)
	//if err != nil {
	//	return err
	//}
	//if err = se.executeDML(ctx, "delete from t where id = %d", rowID); err != nil {
	//	return err
	//}
	//return se.executeDML(ctx, "insert t values (%d, %d, %d, %d)",
	//	rowID, se.ran.nextUniqueIndex(), rowID, cnt+2)
}

func (se *Session) replace(ctx context.Context) error {
	return nil
	// TODO: add replace for multiple tables
	//rowID := se.ran.nextRowID()
	//row := se.conn.QueryRowContext(ctx, fmt.Sprintf("select c from t where id = %d for update", rowID))
	//var cnt int64
	//err := row.Scan(&cnt)
	//if err != nil {
	//	return err
	//}
	//// When replace on existing records, the semantic is equal to `delete then insert`, so the cnt
	//return se.executeDML(ctx, "replace t values (%d, %d, %d, %d)",
	//	rowID, se.ran.nextUniqueIndex(), rowID, cnt+2)
}

func (se *Session) updateSimple(ctx context.Context) error {
	fromTableID, toTableID, fromRowID, toRowID := se.ran.nextTableAndRowPairs()
	if err := se.executeDML(ctx, "update t%d set c = c - 1 where id = %d", fromTableID, fromRowID); err != nil {
		return err
	}
	return se.executeDML(ctx, "update t%d set c = c + 1 where id = %d", toTableID, toRowID)
}

func (se *Session) updateIndex(ctx context.Context) error {
	fromTableID, toTableID, fromRowID, toRowID := se.ran.nextTableAndRowPairs()
	if err := se.executeDML(ctx, "update t%d set i = i + 1, c = c - 1 where id = %d", fromTableID, fromRowID); err != nil {
		return err
	}
	return se.executeDML(ctx, "update t%d set i = i + 1, c = c + 1 where id = %d", toTableID, toRowID)
}

// updateUniqueIndex make sure there is no conflict on the unique index by randomly generate the unique index value
func (se *Session) updateUniqueIndex(ctx context.Context) error {
	return se.executeDML(ctx, "update t%d set u = %d, c = c where id = %d",
		se.ran.nextTableID(), se.ran.nextUniqueIndex(), se.ran.nextRowID())
}

func (se *Session) updateRange(ctx context.Context) error {
	fromTableID, toTableID, fromRowID, toRowID := se.ran.nextTableAndRowPairs()
	beginRowID := mathutil.MinUint64(fromRowID, toRowID) + 2
	if err := se.executeDML(ctx, "update t%d set c = c - 1 where id between %d and %d", fromTableID, beginRowID, beginRowID+8); err != nil {
		return err
	}
	return se.executeDML(ctx, "update t%d set c = c + 1 where id between %d and %d", toTableID, beginRowID, beginRowID+8)
}

func (se *Session) updateIndexRange(ctx context.Context) error {
	fromTableID, toTableID, fromRowID, toRowID := se.ran.nextTableAndRowPairs()
	beginRowID := mathutil.MinUint64(fromRowID, toRowID) + 2
	if err := se.executeDML(ctx, "update t%d set i = i + 1, c = c - 1 where id between %d and %d", fromTableID, beginRowID, beginRowID+8); err != nil {
		return err
	}
	return se.executeDML(ctx, "update t%d set i = i + 1, c = c + 1 where id between %d and %d", toTableID, beginRowID, beginRowID+8)
}

func (se *Session) plainSelect(ctx context.Context) error {
	tableID := se.ran.nextTableID()
	beginRowID := se.ran.nextRowID()
	endRowID := beginRowID + 10
	return se.executeSelect(ctx, "select * from t%d where id between %d and %d", tableID, beginRowID, endRowID)
}

func (se *Session) selectForUpdate(ctx context.Context) error {
	return se.executeSelect(ctx, "select * from t%d where id in (%d, %d) for update",
		se.ran.nextTableID(), se.ran.nextRowID(), se.ran.nextRowID())
}

func (se *Session) executeDML(ctx context.Context, sqlFormat string, args ...interface{}) error {
	sql := fmt.Sprintf(sqlFormat, args...)
	res, err := se.conn.ExecContext(ctx, sql)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return errors.New("affected row is 0, " + sql)
	}
	se.addedCount += int(affected)
	return nil
}

func (se *Session) executeSelect(ctx context.Context, sqlFormat string, args ...interface{}) error {
	sql := fmt.Sprintf(sqlFormat, args...)
	rows, err := se.conn.QueryContext(ctx, sql)
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return rows.Close()
}

func (se *Session) executeCommit(ctx context.Context) error {
	_, err := se.conn.ExecContext(ctx, "commit")
	return err
}

func (se *Session) reset() {
	se.ran.allocated = map[uint64]struct{}{}
	se.addedCount = 0
	se.txnStart = time.Now()
}

// randIDGenerator generates random ID that combines round-robin and zipf distribution and make sure not duplicated.
type randIDGenerator struct {
	allocated     map[uint64]struct{}
	zipf          *rand.Zipf
	uniform       *rand.Rand
	tableNum      int
	max           uint64
	partitionSize uint64
	numPartitions uint64
	partitionIdx  uint64
}

func (r *randIDGenerator) nextTableAndRowPairs() (fromTableID, toTableID int, fromRowID, toRowID uint64) {
	fromTableID = r.nextTableID()
	toTableID = r.nextTableID()
	fromRowID = r.nextRowID()
	toRowID = r.nextRowID()
	return
}

func (r *randIDGenerator) nextTableID() int {
	return r.uniform.Intn(r.tableNum)
}

func (r *randIDGenerator) nextRowID() uint64 {
	return r.uniform.Uint64() % *tableSize
}

func (r *randIDGenerator) reset() {
	r.allocated = map[uint64]struct{}{}
}

func (r *randIDGenerator) nextNumStatements(n int) int {
	return r.uniform.Intn(n)
}

func (r *randIDGenerator) nextUniqueIndex() uint64 {
	return r.uniform.Uint64()
}

func newRandIDGenerator(maxSize uint64, numPartitions uint64) *randIDGenerator {
	partitionSize := maxSize / numPartitions
	src := rand.NewSource(time.Now().UnixNano())
	ran := rand.New(src)
	zipf := rand.NewZipf(ran, 1.01, 1, partitionSize-1)
	return &randIDGenerator{
		allocated:     map[uint64]struct{}{},
		zipf:          zipf,
		uniform:       ran,
		max:           maxSize,
		tableNum:      *tables,
		partitionSize: maxSize / numPartitions,
		numPartitions: numPartitions,
	}
}

func statsLoop() {
	ticker := time.NewTicker(time.Second * 10)
	lastSuccess := uint64(0)
	lastFail := uint64(0)
	for {
		<-ticker.C
		curSuccess := atomic.LoadUint64(&successTxn)
		curFail := atomic.LoadUint64(&failTxn)
		log.Printf("tps(success:%v fail:%v)\n", float64(curSuccess-lastSuccess)/10, float64(curFail-lastFail)/10)
		lastSuccess = curSuccess
		lastFail = curFail
	}
}

func checkLoop(db *sql.DB) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		panic(err)
	}
	ticker := time.NewTicker(time.Second * 30)
	tableNames := make([]string, *tables)
	for i := 0; i < *tables; i++ {
		tableNames[i] = fmt.Sprintf("select c from t%d", i)
	}
	allSumStmt := fmt.Sprintf("select sum(c) from (%s) tall", strings.Join(tableNames, " union all "))

	for {
		<-ticker.C
		checkCount(conn, allSumStmt, 0)
		for i := 0; i < *tables; i++ {
			checkCount(conn, fmt.Sprintf("select count(*) from t%d use index (i)", i), int64(*tableSize))
			checkCount(conn, fmt.Sprintf("select count(*) from t%d use index (u)", i), int64(*tableSize))
		}
	}
}

func checkCount(conn *sql.Conn, sql string, expected int64) {
	row := conn.QueryRowContext(context.Background(), sql)
	var c int64
	if err := row.Scan(&c); err != nil {
		log.Println("check", sql, err)
	} else {
		if c != expected {
			panic(fmt.Sprintf("data inconsistency, %s is %d, expecte %d", sql, c, expected))
		}
	}
}
