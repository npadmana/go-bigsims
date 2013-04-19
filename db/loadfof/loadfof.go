/* loadfof demonstrates loading in an FoF file

Nikhil Padmanabhan, Yale
*/
package main

import (
	"flag"
	"fmt"
	"github.com/npadmana/go-bigsims/db"
	"github.com/npadmana/go-bigsims/db/gioprint"
	"log"
	"math"
)

func main() {
	var genericPath, fofName string
	var help bool
	var err error

	// Setup flags
	flag.BoolVar(&help, "help", false, "help")
	flag.StringVar(&genericPath, "gio", "", "path to genericIOPrint")
	flag.StringVar(&fofName, "fof", "", "FoF filename")
	flag.Parse()
	if help {
		flag.Usage()
	}
	if fofName == "" {
		log.Fatal("Specify the FoF file name")
	}

	// Information
	fmt.Println("FoF loader example")

	// Read the file
	gp := gioprint.NewGIOPrinter(genericPath)
	halo := new(db.Halos)
	if err = gp.Exec(fofName, halo); err != nil {
		log.Fatal(err)
	}

	fmt.Println("File read....")

	numhalos := 0.0
	mass := 0.0
	halo.ForEach(func(h db.Halo) {
		mass += math.Log10(h.Mass)
		numhalos += 1
	})

	fmt.Printf("The file has %d halos, with a mean log mass of %f\n", int(numhalos), mass/numhalos)
}
