/* loadfof processes an FoF file and builds an simple 
SQLite database from it

Nikhil Padmanabhan, Yale
*/
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	//"os"
	"os/exec"
	"path"
)

func main() {
	var genericPath, fofName, dbName string
	var help bool
	var err error

	// Setup flags
	flag.BoolVar(&help, "help", false, "help")
	flag.StringVar(&genericPath, "gio", "", "path to genericIOPrint")
	flag.StringVar(&fofName, "fof", "", "FoF filename")
	flag.StringVar(&dbName, "db", "", "SQLite dbname")
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

	// Loop over the lines, reading from the file
	var mass, xcen, ycen, zcen, xmean, ymean, zmean, vx, vy, vz, vdisp float64
	var hcount, htag int64
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
		fmt.Fscanf(breader, "%d %d", &hcount, &htag)
		fmt.Fscanf(breader, "%f %f %f %f", &mass, &xcen, &ycen, &zcen)
		fmt.Fscanf(breader, "%f %f %f", &xmean, &ymean, &zmean)
		fmt.Fscanf(breader, "%f %f %f %f", &vx, &vy, &vz, &vdisp)

		fmt.Println(hcount, htag, mass, Pxcen, ycen, zcen, vx, vy, vz, vdisp)
	}
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
}
