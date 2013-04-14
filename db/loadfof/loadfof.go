/* loadfof processes an FoF file and builds an simple 
SQLite database from it

Nikhil Padmanabhan, Yale
*/
package main

import (
	"bufio"
	"flag"
	"fmt"
	//"io"
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
	err = nil
	var str string
	for err == nil {
		str, err = fbuf.ReadString('\n')
		fmt.Print(str)
	}
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
}
