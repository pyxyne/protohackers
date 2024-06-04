package main

import "pyxyne/protohackers/lib"

func P0() {
	lib.ServeTcp(func(c *lib.Client) (err error) {
		for c.HasData() {
			msg, err := c.ReadAny()
			if err != nil {
				return err
			}
			c.WriteAll(msg)
			c.Log.Debug("Echoed %d bytes", len(msg))
		}
		return nil
	})
}
