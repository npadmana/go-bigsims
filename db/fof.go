package db

import (
	"bytes"
	"fmt"
	"github.com/npadmana/go-bigsims/db/gioprint"
	"path/filepath"
	"sync"
)

/* Define the FoF structure
 */
type Halo struct {
	Tag, Count          int64
	Mass                float64
	Xcen, Ycen, Zcen    float64
	Xmean, Ymean, Zmean float64
	Vx, Vy, Vz, Vdisp   float64
}

type Halos struct {
	Cat [][]Halo
}

func (h *Halos) AllocRanks(nrank int) {
	h.Cat = make([][]Halo, nrank)
}

func (h *Halos) InitRank(irank, nobj int) {
	h.Cat[irank] = make([]Halo, nobj)
}

func (h *Halos) Read(irank, iobj int, arr []byte) (err error) {
	breader := bytes.NewBuffer(arr)
	_, err = fmt.Fscanf(breader, "%d %d", &h.Cat[irank][iobj].Count, &h.Cat[irank][iobj].Tag)
	if err != nil {
		return err
	}
	_, err = fmt.Fscanf(breader, "%f %f %f %f", &h.Cat[irank][iobj].Mass, &h.Cat[irank][iobj].Xcen, &h.Cat[irank][iobj].Ycen, &h.Cat[irank][iobj].Zcen)
	if err != nil {
		return err
	}
	_, err = fmt.Fscanf(breader, "%f %f %f", &h.Cat[irank][iobj].Xmean, &h.Cat[irank][iobj].Ymean, &h.Cat[irank][iobj].Zmean)
	if err != nil {
		return err
	}
	_, err = fmt.Fscanf(breader, "%f %f %f %f", &h.Cat[irank][iobj].Vx, &h.Cat[irank][iobj].Vy, &h.Cat[irank][iobj].Vz, &h.Cat[irank][iobj].Vdisp)
	if err != nil {
		return err
	}
	return nil
}

func (h *Halos) Nranks() int {
	return len(h.Cat)
}

func (h *Halos) ForEach(ff func(Halo)) {
	for _, rank := range h.Cat {
		for _, obj := range rank {
			ff(obj)
		}
	}
}

func (h *Halos) ReadFile(gp *gioprint.GIOPrinter, fn string, nreaders int) error {
	// Glob all the files
	fns, err := filepath.Glob(fn + "#*")
	if err != nil {
		return err
	}
	nfiles := len(fns)

	// Allocate temporary storage for each of the files
	tmp := make([]Halos, nfiles)
	chans := make([]chan int, nreaders)
	var wg sync.WaitGroup

	// Launch the readers 
	for i := 0; i < nreaders; i++ {
		wg.Add(1)
		chans[i] = make(chan int)
		go func(fnlist []string, hlist []Halos, ch chan int) {
			ok := true
			var i int
			for {
				i, ok = <-ch
				if !ok {
					wg.Done()
					return
				}
				if err = gp.Exec(fnlist[i], &hlist[i], false); err != nil {
					panic(err)
				}
			}
		}(fns, tmp, chans[i])
	}

	nextreader := 0
	var ok bool
	for i := range fns {
		ok = false
		for !ok {
			select {
			case chans[nextreader] <- i:
				ok = true
			default:
			}
			nextreader = (nextreader + 1) % nreaders
		}
	}

	for _, ch := range chans {
		close(ch)
	}

	wg.Wait()

	// Now calculate number of ranks
	totranks := 0
	for _, tmp1 := range tmp {
		totranks += tmp1.Nranks()
	}

	// Collect everything together
	h.AllocRanks(totranks)
	startpos := 0
	nrank1 := 0
	for _, tmp1 := range tmp {
		nrank1 = tmp1.Nranks()
		copy(h.Cat[startpos:startpos+nrank1], tmp1.Cat)
		startpos += nrank1
	}

	return nil
}
