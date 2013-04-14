/* loadfof processes an FoF file and builds an simple 
SQLite database from it

Nikhil Padmanabhan, Yale
*/
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
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

	fmt.Println("FoF SQLite loader")
	fmt.Printf("Input file %s ---> Output DB %s\n", fofName, dbName)

	cmd := exec.Command(path.Join(genericPath, "GenericIOPrint"), fofName)
	outpipe, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
	}
	fbuf := bufio.NewReader(outpipe)
	for err != nil {
		str, err := fbuf.ReadString('\n')
		if (err != nil) && (err != io.EOF) {
			fmt.Println(str)
		}
	}
	err = cmd.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
