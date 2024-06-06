package main

import (
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
	lib.ServeTcp(func(c *lib.TcpClient) (err error) {
		prices := make([]Price, 0)

		for c.HasData() {
			r := c.GetBinReader()
			req_ty := r.U8()
			req_a := r.I32BE()
			req_b := r.I32BE()
			if r.Err != nil {
				return r.Err
			}
			switch req_ty {
			case 'I':
				price := Price{req_a, req_b}
				c.Log.Debug("Inserting price at t=%d", price.timestamp)
				i, _ := slices.BinarySearchFunc(prices, price.timestamp, cmpPriceTimestamp)
				prices = slices.Insert(prices, i, price)
			case 'Q':
				i1, _ := slices.BinarySearchFunc(prices, req_a, cmpPriceTimestamp)
				i2, _ := slices.BinarySearchFunc(prices, req_b, cmpPriceTimestamp)
				for i2 < len(prices) && prices[i2].timestamp == req_b {
					i2++
				}
				c.Log.Debug("Querying mean for t in [%d;%d] -> i in [%d;%d]", req_a, req_b, i1, i2)
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
