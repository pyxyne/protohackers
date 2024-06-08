package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"pyxyne/protohackers/lib"
	"reflect"
	"slices"
	"sync"
	"time"
)

func readStr(r *lib.BinReader) string {
	len := r.U8()
	return string(r.Bytes(int(len)))
}

const (
	ROLE_UNKNOWN = iota
	ROLE_CAMERA
	ROLE_DISPATCHER
)

type Plate struct {
	road      uint16
	limit     uint16
	mile      uint16
	plate     string
	timestamp uint32
}
type Car struct {
	road  uint16
	plate string
}
type CarPos struct {
	mile      uint16
	timestamp uint32
}
type CarDay struct {
	plate string
	day   uint32
}

func P6() {
	log := lib.Logger{}

	roadTicketsMutex := sync.Mutex{}
	roadTickets := make(map[uint16]chan []byte)
	getTicketChan := func(road uint16) chan []byte {
		roadTicketsMutex.Lock()
		ticketChan, ok := roadTickets[road]
		if !ok {
			ticketChan = make(chan []byte, 10)
			roadTickets[road] = ticketChan
		}
		roadTicketsMutex.Unlock()
		return ticketChan
	}

	selectTickets := func(roads []uint16, ctx context.Context) chan []byte {
		cases := make([]reflect.SelectCase, 0, len(roads)+1)
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctx.Done()),
		})
		for _, road := range roads {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(getTicketChan(road)),
			})
		}
		out := make(chan []byte)
		go func() {
			for {
				case_idx, val, ok := reflect.Select(cases)
				if case_idx == 0 {
					lib.Assert(!ok)
					close(out)
					return
				}
				out <- val.Interface().([]byte)
			}
		}()
		return out
	}

	ticketed := make(map[CarDay]bool)
	checkSpeed := func(plate string, road uint16, limit uint16, pos1 CarPos, pos2 CarPos) {
		dmile := math.Abs(float64(pos2.mile) - float64(pos1.mile))
		dt := (float64(pos2.timestamp) - float64(pos1.timestamp)) / 3600
		speed := dmile / dt
		if speed < float64(limit)+0.25 {
			return
		}

		cd1 := CarDay{plate, pos1.timestamp / 3600 / 24}
		cd2 := CarDay{plate, pos2.timestamp / 3600 / 24}
		if ticketed[cd1] || ticketed[cd2] {
			return
		}

		log.Debug("ticket: plate=%q, road=%d, pos1=%v, pos2=%v, speed=%.2f", plate, road, pos1, pos2, speed)
		var w lib.BinWriter
		w.U8(0x21)
		w.U8(uint8(len(plate)))
		w.Bytes([]byte(plate))
		w.U16BE(road)
		w.U16BE(pos1.mile)
		w.U32BE(pos1.timestamp)
		w.U16BE(pos2.mile)
		w.U32BE(pos2.timestamp)
		w.U16BE(uint16(math.Round(speed * 100)))
		select {
		case getTicketChan(road) <- w.Buf:
		default:
			panic("ticket channel buffer overflow")
		}

		ticketed[cd1] = true
		ticketed[cd2] = true
	}

	plateChan := make(chan Plate)
	plateProcessor := func() {
		positions := make(map[Car][]CarPos)
		for plate := range plateChan {
			car := Car{plate.road, plate.plate}
			pos := CarPos{plate.mile, plate.timestamp}
			history, ok := positions[car]
			if !ok {
				history = make([]CarPos, 0, 4)
			}
			i, found := slices.BinarySearchFunc(history, plate.timestamp, func(pos CarPos, t uint32) int {
				return cmp.Compare(pos.timestamp, t)
			})
			lib.Assert(!found)
			if i > 0 {
				checkSpeed(plate.plate, plate.road, plate.limit, history[i-1], pos)
			}
			if i < len(history) {
				checkSpeed(plate.plate, plate.road, plate.limit, pos, history[i])
			}
			history = slices.Insert(history, i, pos)
			positions[car] = history
		}
	}
	go plateProcessor()

	lib.ServeTcp(func(c *lib.TcpClient) error {
		role := ROLE_UNKNOWN
		heartbeatSet := false
		var road, mile, limit uint16
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		runHeartbeat := func(interval_ds uint32) {
			interval := time.Duration(100*interval_ds) * time.Millisecond
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				c.Log.Debug("-> heartbeat")
				c.WriteAll([]byte{0x41})
				time.Sleep(interval)
			}
		}

		runDispatcher := func(roads []uint16) {
			for ticket := range selectTickets(roads, ctx) {
				c.Log.Debug("-> ticket")
				c.WriteAll(ticket)
			}
		}

		var processMsg = func() error {
			r := c.GetBinReader()
			msg_ty := r.U8()
			if r.Err != nil {
				return r.Err
			}
			switch msg_ty {
			case 0x20: // Plate
				plate := readStr(r)
				timestamp := r.U32BE()
				if r.Err != nil {
					return r.Err
				}
				c.Log.Debug("<- Plate(%q, %d)", plate, timestamp)
				if role != ROLE_CAMERA {
					return errors.New("observation received from non-camera")
				}
				plateChan <- Plate{road, limit, mile, plate, timestamp}
			case 0x40: // WantHeartbeat
				interval := r.U32BE()
				if r.Err != nil {
					return r.Err
				}
				c.Log.Debug("<- WantHeartbeat(%d)", interval)
				if heartbeatSet {
					return errors.New("client requested heartbeat twice")
				}
				heartbeatSet = true
				if interval != 0 {
					go runHeartbeat(interval)
				}
			case 0x80: // IAmCamera
				road = r.U16BE()
				mile = r.U16BE()
				limit = r.U16BE()
				if r.Err != nil {
					return r.Err
				}
				c.Log.Debug("<- IAmCamera(road=%d, mile=%d, limit=%d)", road, mile, limit)
				if role != ROLE_UNKNOWN {
					return errors.New("client identified twice")
				}
				c.Log.Name = fmt.Sprintf("camera%d_%d", road, mile)
				role = ROLE_CAMERA
			case 0x81: // IAmDispatcher
				road_cnt := r.U8()
				roads := make([]uint16, 0, road_cnt)
				for range road_cnt {
					roads = append(roads, r.U16BE())
				}
				if r.Err != nil {
					return r.Err
				}
				c.Log.Debug("<- IAmDispatcher(roads=%v)", roads)
				if role != ROLE_UNKNOWN {
					return errors.New("client identified twice")
				}
				c.Log.Name = fmt.Sprintf("dispatcher%s", c.Log.Name[4:])
				role = ROLE_DISPATCHER
				go runDispatcher(roads)
			default:
				return errors.New("invalid message type")
			}
			return nil
		}

		for c.HasData() {
			err := processMsg()
			if err != nil {
				msg := err.Error()
				len := uint8(len(msg))
				c.WriteAll(slices.Concat([]byte{0x10, len}, []byte(msg[:len])))
				c.Close()
				return err
			}
		}
		return nil
	})
}
