package main

import (
	"encoding/json"
	"fmt"
	"math"
	"pyxyne/protohackers/lib"
)

func isPrime(xf float64) bool {
	if math.Floor(xf) != xf || xf < 2.0 || xf >= float64(1<<64) {
		// not a (representable) integer
		return false
	}
	x := uint64(xf)
	if x%2 == 0 {
		return x == 2
	}
	for k := uint64(3); k*k <= x; k += 2 {
		if x%k == 0 {
			return false
		}
	}
	return true
}

type Req struct {
	Method string
	Number float64
}

func P1() {
	lib.ServeTcp(func(c *lib.Client) (err error) {
		for c.HasData() {
			msg, err := c.ReadLine()
			if err != nil {
				return err
			}
			c.Log.Debug("Request: %#v", string(msg))
			req := Req{"", math.NaN()}
			err = json.Unmarshal([]byte(msg), &req)
			if err != nil || req.Method != "isPrime" || math.IsNaN(req.Number) {
				c.Log.Warn("Malformed request")
				c.WriteLine("\"malformed request\"")
				return nil
			}
			res := isPrime(req.Number)
			c.Log.Debug("Response: %v", res)
			c.WriteLine(fmt.Sprintf("{\"method\":\"isPrime\",\"prime\":%t}\n", res))
		}
		return nil
	})
}
