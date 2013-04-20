/* A set of utility wrappers for the GenericIOPrint utility 

Nikhil Padmanabhan, Yale
*/
package gioprint

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"strconv"
	"time"
)

type GIOPrinter struct {
	cmdname string
}

/* A general interface to handle that data that comes out from GenericIOPrint 

The data are divided into nranks, and each rank has n entries. The information 
for these is in various headers that come out of the stream. 

A natural organization for the data is therefore a 2D slice
[nranks][n]Elements

The three methods help do this :
  -- AllocRanks allows you to allocate based on the number of ranks
  -- InitRank allows you to initialize a particular rank
  -- Read allows you to fill a particular element.

  You may decide some of these can be dummy functions, eg. if you just want to stream through the data
*/
type GIOScanner interface {
	AllocRanks(nrank int)                   // Function that performs any allocations based on the number of ranks
	InitRank(irank, nobj int)               // Initialize rank i for nobj objects
	Read(irank, iobj int, arr []byte) error // Read into object irank, iobj from arr
}

func NewGIOPrinter(dir string) (gp *GIOPrinter) {
	gp = new(GIOPrinter)
	gp.cmdname = path.Join(dir, "GenericIOPrint")
	return
}

func parserank(barr []byte) (int, error) {
	ndx1 := bytes.IndexAny(barr, ":")
	if ndx1 == -1 {
		return 0, errors.New("Malformed rank line, could not find rank")
	}
	barr = barr[ndx1+1:]
	ndx1 = bytes.IndexAny(barr, ":")
	if ndx1 == -1 {
		return 0, errors.New("Malformed rank line, could not find rank")
	}
	barr = barr[ndx1+1:]

	// Find rows
	ndx1 = bytes.Index(barr, []byte("row"))
	if ndx1 == -1 {
		return 0, errors.New("Malformed rank line, could not find row")
	}

	// Attempt to parse the row
	nobj, err := strconv.ParseInt(string(bytes.TrimSpace(barr[:ndx1])), 10, 32)
	if err != nil {
		return 0, err
	}
	return int(nobj), nil
}

func parsehdr(barr []byte, fn string) (int, error) {
	barr = bytes.TrimSpace(barr)
	// First character is a #
	if barr[0] != '#' {
		return 0, errors.New("Missing # to start header")
	}
	barr = barr[1:]

	// Second elt is the filename
	ndx1 := bytes.IndexAny(barr, ":")
	if ndx1 == -1 {
		return 0, errors.New("Malformed header, couldn't find filename")
	}
	if bytes.Compare(bytes.TrimSpace(barr[:ndx1]), []byte(fn)) != 0 {
		return 0, errors.New("Malformed header, unable to find filename in header")
	}

	// Find ranks
	barr = barr[ndx1+1:]
	ndx1 = bytes.Index(barr, []byte("rank"))
	if ndx1 == -1 {
		return 0, errors.New("Malformed header, could not find rank")
	}

	// Attempt to parse the rank
	rank64, err := strconv.ParseInt(string(bytes.TrimSpace(barr[:ndx1])), 10, 32)
	if err != nil {
		return 0, err
	}
	return int(rank64), nil
}

/* Read the file */
func (gp *GIOPrinter) Exec(fn string, scanner GIOScanner, verbose bool) error {
	// Start reading the file
	cmd := exec.Command(gp.cmdname, fn)
	outpipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	fbuf := bufio.NewReader(outpipe)
	if err := cmd.Start(); err != nil {
		return err
	}

	var barr []byte // Byte buffer

	// Read the first line and get the number of ranks
	barr, err = fbuf.ReadBytes('\n')
	if err != nil {
		return err
	}
	nrank, err := parsehdr(barr, fn)
	if err != nil {
		return err
	}
	// Call AllocRanks
	scanner.AllocRanks(nrank)
	if verbose {
		fmt.Printf("Expecting %d ranks...\n", nrank)
	}
	var nobj int
	tstart := time.Now()
	for irank := 0; irank < nrank; irank++ {

		// Find the rank header
		found := false
		for !found {
			barr, err = fbuf.ReadBytes('\n')
			if err != nil {
				return errors.New("Unexpected stream truncation")
			}
			// Is this prefixed by rank
			barr = bytes.TrimSpace(barr)
			if bytes.HasPrefix(barr, []byte("# rank")) {
				nobj, err = parserank(barr[6:])
				if err != nil {
					return err
				}
				found = true
				continue
			}
			if barr[0] != '#' {
				return errors.New("Unexpected line, not a comment, not in data section")
			}
		}

		// Initialize and read in block of data
		scanner.InitRank(irank, nobj)
		for iobj := 0; iobj < nobj; iobj++ {
			barr, err = fbuf.ReadBytes('\n')
			if err != nil {
				return err
			}
			if err = scanner.Read(irank, iobj, barr); err != nil {
				return err
			}
		}

		// Verbosity
		if verbose && ((irank % 1000) == 0) {
			fmt.Printf("%d rank just completed, elapsed time is %v .... \n", irank, time.Since(tstart))
		}
	}

	if err = cmd.Wait(); err != nil {
		return err
	}

	return nil
}
