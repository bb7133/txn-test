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
)

var (
	addr         = flag.String("addr", "127.0.0.1:4000", "tidb address")
	concurrency  = flag.Int("concurrency", 32, "concurrency")
	loadData     = flag.Bool("load-data", false, "load data before run")
	tableSize    = flag.Uint64("table-size", 400000, "table size")
	ignoreO      = flag.String("ignore-o", "9007,1105", "ignored error code for optimistic transaction, separated by comma")
	ignoreP      = flag.String("ignore-p", "1213", "ignored error code for pessimistic transaction, separated by comma")
	mode         = flag.String("mode", "mix", "transaction mode, mix|pessimistic|optimistic")
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
	db.Exec("drop table if exists t")
	db.Exec("create table t (id bigint primary key, u bigint unsigned unique, i bigint, c bigint, index i (i))")
	for i := uint64(0); i < maxSize; i += batchSize {
		_, err := db.Exec(insertSql(i))
		if err != nil {
			return err
		}
	}
	return nil
}

func insertSql(beginID uint64) string {
	var values []string
	for i := beginID; i < beginID+batchSize; i++ {
		value := fmt.Sprintf("(%d, %d, %d, %d)", i, i, i, 0)
		values = append(values, value)
	}
	return "insert t values " + strings.Join(values, ",")
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
		se.updateNonIndex,
		se.updateIndex,
		se.updateIndexRange,
		se.updateRange,
		se.updateUniqueIndex,
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
	// Make sure the addedCount is subtracted
	if se.addedCount > 0 {
		err = se.executeDML(ctx, "update t set c = c - %d where id = %d", se.addedCount, se.ran.nextRowID())
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

func (se *Session) runInsertDeleteTransaction(parent context.Context) error {
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
	for i := 0; i < 4; i++ {
		rowID := se.ran.nextRowID()
		selectSQL := fmt.Sprintf("select c from t where id = %d for update", rowID)
		row := se.conn.QueryRowContext(ctx, selectSQL)
		var c int64
		err = row.Scan(&c)
		if err == sql.ErrNoRows {
			insertSQL := fmt.Sprintf("insert t values (%d, %d, %d, 0)", rowID, rowID, rowID)
			_, err = se.conn.ExecContext(ctx, insertSQL)
			if err != nil {
				if getErrorCode(err) != 1062 {
					log.Println("insert err", err)
				}
			}
		} else {
			deleteSQL := fmt.Sprintf("delete from t where id = %d", rowID)
			_, err = se.conn.ExecContext(ctx, deleteSQL)
			if err != nil {
				log.Println("delete err", err)
			}
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
	if *insertDelete {
		runFunc = se.runInsertDeleteTransaction
	}
	for {
		if err := runFunc(ctx); err != nil {
			log.Println("begin error", err)
			return
		}
	}
}

func (se *Session) deleteInsert(ctx context.Context) error {
	rowID := se.ran.nextRowID()
	row := se.conn.QueryRowContext(ctx, fmt.Sprintf("select c from t where id = %d for update", rowID))
	var cnt int64
	err := row.Scan(&cnt)
	if err != nil {
		return err
	}
	if err = se.executeDML(ctx, "delete from t where id = %d", rowID); err != nil {
		return err
	}
	return se.executeDML(ctx, "insert t values (%d, %d, %d, %d)",
		rowID, se.ran.nextUniqueIndex(), rowID, cnt+2)
}

func (se *Session) updateIndex(ctx context.Context) error {
	return se.executeDML(ctx, "update t set i = i + 1, c = c + 1 where id in (%d, %d)", se.ran.nextRowID(), se.ran.nextRowID())
}

func (se *Session) updateNonIndex(ctx context.Context) error {
	return se.executeDML(ctx, "update t set c = c + 1 where id in (%d, %d)", se.ran.nextRowID(), se.ran.nextRowID())
}

// updateUniqueIndex make sure there is no conflict on the unique index by randomly generate the unique index value
func (se *Session) updateUniqueIndex(ctx context.Context) error {
	return se.executeDML(ctx, "update t set u = %d, c = c + 1 where id = %d",
		se.ran.nextUniqueIndex(), se.ran.nextRowID())
}

func (se *Session) updateRange(ctx context.Context) error {
	beginRowID := se.ran.nextRowID() + 2 // add 2 to avoid high conflict rate.
	endRowID := beginRowID + 10
	return se.executeDML(ctx, "update t set c = c + 1 where id between %d and %d", beginRowID, endRowID)
}

func (se *Session) updateIndexRange(ctx context.Context) error {
	beginRowID := se.ran.nextRowID() + 2 // add 2 to avoid high conflict rate.
	endRowID := beginRowID + 10
	return se.executeDML(ctx, "update t set i = i + 1, c = c + 1 where id between %d and %d", beginRowID, endRowID)
}

func (se *Session) plainSelect(ctx context.Context) error {
	beginRowID := se.ran.nextRowID()
	endRowID := beginRowID + 10
	return se.executeSelect(ctx, "select * from t where id between %d and %d", beginRowID, endRowID)
}

func (se *Session) selectForUpdate(ctx context.Context) error {
	return se.executeSelect(ctx, "select * from t where id in (%d, %d) for update",
		se.ran.nextRowID(), se.ran.nextRowID())
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
	max           uint64
	partitionSize uint64
	numPartitions uint64
	partitionIdx  uint64
}

func (r *randIDGenerator) nextRowID() uint64 {
	r.partitionIdx = (r.partitionIdx + 1) % r.numPartitions
	for {
		v := r.zipf.Uint64() + r.partitionIdx*r.partitionSize
		if _, ok := r.allocated[v]; ok {
			continue
		}
		r.allocated[v] = struct{}{}
		return v
	}
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
	for {
		<-ticker.C
		checkCount(conn, "select sum(c) from t", 0)
		checkCount(conn, "select count(*) from t use index (i)", int64(*tableSize))
		checkCount(conn, "select count(*) from t use index (u)", int64(*tableSize))
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
