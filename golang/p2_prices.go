package main

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"pyxyne/protohackers/lib"
	"slices"
)

type Price struct {
	timestamp int32
	price     int32
}

func cmpPriceTimestamp(p Price, t int32) int {
	return cmp.Compare(p.timestamp, t)
}

func P2() {
	lib.ServeTcp(func(c *lib.Client) (err error) {
		prices := make([]Price, 0)

		for c.HasData() {
			msg, err := c.ReadExactly(9)
			if err != nil {
				return err
			}
			var req struct {
				T uint8
				A int32
				B int32
			}
			binary.Read(bytes.NewReader(msg), binary.BigEndian, &req)
			switch req.T {
			case 'I':
				price := Price{req.A, req.B}
				c.Log.Debug("Inserting price at t=%d", price.timestamp)
				i, _ := slices.BinarySearchFunc(prices, price.timestamp, cmpPriceTimestamp)
				prices = slices.Insert(prices, i, price)
			case 'Q':
				i1, _ := slices.BinarySearchFunc(prices, req.A, cmpPriceTimestamp)
				i2, _ := slices.BinarySearchFunc(prices, req.B, cmpPriceTimestamp)
				for i2 < len(prices) && prices[i2].timestamp == req.B {
					i2++
				}
				c.Log.Debug("Querying mean for t in [%d;%d] -> i in [%d;%d]", req.A, req.B, i1, i2)
				var mean int32
				if i1 >= i2 {
					mean = 0
				} else {
					sum := int64(0)
					for _, p := range prices[i1:i2] {
						sum += int64(p.price)
					}
					mean = int32(sum / int64(i2-i1))
				}
				c.Log.Debug("-> Mean = %d", mean)
				buf := [4]byte{}
				binary.BigEndian.AppendUint32(buf[:0], uint32(mean))
				c.WriteAll(buf[:])
			}
		}
		return nil
	})
}
