package main

import (
	"fmt"
	"pyxyne/protohackers/lib"
	"slices"
	"sync"
)

func P4() {
	db := make(map[string]string)
	db["version"] = "pyxyne's db"
	mutex := sync.RWMutex{}
	var insert = func(key string, val string) {
		if key == "version" {
			return
		}
		mutex.Lock()
		defer mutex.Unlock()
		db[key] = val
	}
	var retrieve = func(key string) string {
		mutex.RLock()
		defer mutex.RUnlock()
		return db[key]
	}

	lib.ServeUdp(func(c *lib.UdpClient) (err error) {
		for {
			msg := c.ReadMsg()
			if msg == nil {
				break
			}
			c.Log.Debug("<- %q", msg)
			i := slices.Index(msg, '=')
			if i != -1 {
				insert(string(msg[:i]), string(msg[i+1:]))
			} else {
				key := string(msg)
				val := retrieve(key)
				c.Log.Debug("-> %q", val)
				err := c.SendMsg([]byte(fmt.Sprintf("%s=%s", key, val)))
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}
