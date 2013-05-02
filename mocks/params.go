// Package mocks has all the code to generate mock catalogs
package mocks

import (
	"encoding/json"
	"io"
)

// MockParams defines the parameters of the mock 
type MockParams struct {
	HODType  string     // What kind of HOD to use
	Geometry string     // Defines the geometry
	Origin   [3]float64 // Where to put the origin

	// Things below this line may or may not be set, depending 
	// on the input file.
	Shell ShellType
}

// Defines a shell, between Rmin to Rmax 
type ShellType struct {
	Rmin, Rmax float64
}

//Function ReadParams reads in a JSON param file and decodes into the MockParam structure.
func ReadParams(paramf io.Reader) (MockParams, error) {
	var m MockParams

	dec := json.NewDecoder(paramf)
	if err:=dec.Decode(&m);err != nil {
		return m, err
	}

	return m, nil
}
