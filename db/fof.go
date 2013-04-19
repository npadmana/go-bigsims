package db

import (
	"bytes"
	"fmt"
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

func (h *Halos) ForEach(ff func(Halo)) {
	for _, rank := range h.Cat {
		for _, obj := range rank {
			ff(obj)
		}
	}
}
