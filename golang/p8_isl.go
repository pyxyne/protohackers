package main

import (
	"encoding/hex"
	"math/bits"
	"pyxyne/protohackers/lib"
	"slices"
	"strconv"
	"strings"
)

type Opcode uint8

const (
	OP_REV    Opcode = 1
	OP_XOR    Opcode = 2
	OP_XORPOS Opcode = 3
	OP_ADD    Opcode = 4
	OP_ADDPOS Opcode = 5
	OP_SUBPOS Opcode = 6
)

type CypherOp struct {
	op  Opcode
	arg uint8
}

func invertCypher(ops []CypherOp) []CypherOp {
	ops = slices.Clone(ops)
	slices.Reverse(ops)
	for i := range ops {
		switch ops[i].op {
		case OP_ADD:
			ops[i].arg = -ops[i].arg
		case OP_ADDPOS:
			ops[i].op = OP_SUBPOS
		}
	}
	return ops
}

func encrypt(ops []CypherOp, pos0 uint8, bytes []uint8) {
	for _, op := range ops {
		switch op.op {
		case OP_REV:
			for i := range bytes {
				bytes[i] = bits.Reverse8(bytes[i])
			}
		case OP_XOR:
			for i := range bytes {
				bytes[i] ^= op.arg
			}
		case OP_XORPOS:
			for i := range bytes {
				bytes[i] ^= pos0 + uint8(i)
			}
		case OP_ADD:
			for i := range bytes {
				bytes[i] += op.arg
			}
		case OP_ADDPOS:
			for i := range bytes {
				bytes[i] += pos0 + uint8(i)
			}
		case OP_SUBPOS:
			for i := range bytes {
				bytes[i] -= pos0 + uint8(i)
			}
		}
	}
}

func isNoOp(ops []CypherOp) bool {
	bytes := make([]uint8, 256)
	for i := range 256 {
		bytes[i] = uint8(i)
	}
	for pos := range 256 {
		encrypt(ops, uint8(pos), bytes)
		for i, b := range bytes {
			if b != uint8(i) {
				return false
			}
		}
	}
	return true
}

type StreamCypher struct {
	ops []CypherOp
	pos uint8
}

func (c *StreamCypher) encrypt(msg []byte) {
	encrypt(c.ops, c.pos, msg)
	c.pos += uint8(len(msg))
}

func toyCounter(recv chan []byte, send func([]byte)) {
	var buf []byte
	for chunk := range recv {
		buf = append(buf, chunk...)
		for {
			i := slices.Index(buf, '\n')
			if i == -1 {
				break
			}
			line := string(buf[:i])
			buf = slices.Delete(buf, 0, i+1)

			toys := strings.Split(line, ",")
			var bestToy string
			bestToyCnt := uint64(0)
			for _, toy := range toys {
				i := strings.Index(toy, "x ")
				lib.Assert(i != -1)
				cnt, err := strconv.ParseUint(toy[:i], 10, 64)
				lib.CheckPanic(err)
				if cnt > bestToyCnt {
					bestToy = toy
					bestToyCnt = cnt
				}
			}
			send([]byte(bestToy + "\n"))
		}
	}
}

func P8() {
	lib.ServeTcp(func(c *lib.TcpClient) error {
		r := c.GetBinReader()
		var cypher []CypherOp
		for {
			b := r.U8()
			if b == 0 {
				break
			}
			lib.Assert(1 <= b && b <= 5)
			var arg uint8
			if b == byte(OP_XOR) || b == byte(OP_ADD) {
				arg = r.U8()
			}
			cypher = append(cypher, CypherOp{Opcode(b), arg})
		}
		if r.Err != nil {
			return r.Err
		}
		c.Log.Info("Cypher: %v", cypher)
		if isNoOp(cypher) {
			c.Log.Warn("Cypher is no-op")
			return nil
		}
		encode := StreamCypher{cypher, 0}
		decode := StreamCypher{invertCypher(cypher), 0}

		recvChan := make(chan []byte)
		go toyCounter(recvChan, func(msg []byte) {
			c.Log.Debug("--> %q", msg)
			encode.encrypt(msg)
			c.Log.Debug("-> %s", hex.EncodeToString(msg))
			c.WriteAll(msg)
		})

		for c.HasData() {
			msg, err := c.ReadAny()
			if err != nil {
				return err
			}
			c.Log.Debug("<- %s", hex.EncodeToString(msg))
			decode.encrypt(msg)
			c.Log.Debug("<-- %q", msg)
			recvChan <- msg
		}
		close(recvChan)
		return nil
	})
}
