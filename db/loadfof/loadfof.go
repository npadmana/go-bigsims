/* loadfof processes an FoF file and builds an simple 
SQLite database from it

Nikhil Padmanabhan, Yale
*/
package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/npadmana/go-sqlite3"
	"io"
	"log"
	"math"
	"os"
	"os/exec"
	"path"
	"sync"
)

type Halo struct {
	Tag               int64
	Mass              float64
	Xcen, Ycen, Zcen  float64
	Vx, Vy, Vz, Vdisp float64
}

func SQLiteTableInserter(db *sql.DB, tablename string, ch chan Halo, blocksize int, wg *sync.WaitGroup) {

	ok := true
	var err error
	var tx *sql.Tx
	var stmt *sql.Stmt
	var h Halo

	for ok {
		// Prepare the transaction
		tx, err = db.Begin()
		if err != nil {
			log.Fatal(err)
		}

		// Prepare the statement
		stmt, err = tx.Prepare(fmt.Sprintf("insert into %s values(?,?,?,?,?,?,?,?,?)", tablename))
		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < blocksize; i++ {
			h, ok = <-ch
			if !ok {
				break
			} else {
				// Insert into the table
				_, err = stmt.Exec(h.Tag, math.Log10(h.Mass), h.Xcen, h.Ycen, h.Zcen, h.Vx, h.Vy, h.Vz, h.Vdisp)
				if err != nil {
					fmt.Println(i)
					log.Fatal(err)
				}
			}
		}
		tx.Commit()
		stmt.Close()

	}
	wg.Done()

}

func main() {
	var genericPath, fofName, dbName, tablename string
	var help bool
	var err error

	// Setup flags
	flag.BoolVar(&help, "help", false, "help")
	flag.StringVar(&genericPath, "gio", "", "path to genericIOPrint")
	flag.StringVar(&fofName, "fof", "", "FoF filename")
	flag.StringVar(&dbName, "db", "", "SQLite dbname")
	flag.StringVar(&tablename, "table", "fof", "Table name")
	flag.Parse()
	if help {
		flag.Usage()
	}
	if fofName == "" {
		log.Fatal("Specify the FoF file name")
	}
	if dbName == "" {
		log.Fatal("Specify the db name")
	}

	// Information
	fmt.Println("FoF SQLite loader")
	fmt.Printf("Input file %s ---> Output DB %s\n", fofName, dbName)

	// Start reading the file
	cmd := exec.Command(path.Join(genericPath, "GenericIOPrint"), fofName)
	outpipe, err := cmd.StdoutPipe()
	fbuf := bufio.NewReader(outpipe)
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}

	// Set up the database
	os.Remove(dbName)
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create the table
	sqls := []string{
		fmt.Sprintf("create table %s (tag integer, logmass real, xcen real,ycen real,zcen real, vx real, vy real, vz real,vdisp real)", tablename),
	}
	for _, sql1 := range sqls {
		_, err = db.Exec(sql1)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Create a channel 
	ch := make(chan Halo)
	var wg sync.WaitGroup
	wg.Add(1)
	go SQLiteTableInserter(db, tablename, ch, 100000, &wg)

	// Loop over the lines, reading from the file
	var xmean, ymean, zmean float64
	var hcount int64
	var h Halo
	err = nil
	var barr []byte
	commentprefix := []byte("#")
	for err == nil {
		barr, err = fbuf.ReadBytes('\n')

		// Handle errors
		if err == io.EOF {
			continue
		}
		if err != nil {
			log.Fatal(err)
		}

		// See if this is a comment line
		if bytes.HasPrefix(bytes.TrimSpace(barr), commentprefix) {
			continue
		}

		breader := bytes.NewBuffer(barr)
		fmt.Fscanf(breader, "%d %d", &hcount, &h.Tag)
		fmt.Fscanf(breader, "%f %f %f %f", &h.Mass, &h.Xcen, &h.Ycen, &h.Zcen)
		fmt.Fscanf(breader, "%f %f %f", &xmean, &ymean, &zmean)
		fmt.Fscanf(breader, "%f %f %f %f", &h.Vx, &h.Vy, &h.Vz, &h.Vdisp)
		ch <- h

	}

	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}

	close(ch)
	wg.Wait()

	// Create an index on mass
	sqls = []string{
		fmt.Sprintf("create index massindex on %s (logmass)", tablename),
	}
	for _, sql1 := range sqls {
		_, err = db.Exec(sql1)
		if err != nil {
			log.Fatal(err)
		}
	}

}
