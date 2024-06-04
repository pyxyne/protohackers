package main

import (
	"fmt"
	"pyxyne/protohackers/lib"
	"strings"
	"sync"
)

func isValidName(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
}

func P3() {
	mutex := sync.Mutex{}
	nextId := 0
	joined := make(map[int]*lib.Client)

	var broadcast = func(from int, fs string, args ...any) {
		s := fmt.Sprintf(fs, args...)
		mutex.Lock()
		defer mutex.Unlock()
		for id, c := range joined {
			if id != from {
				c.WriteLine(s)
			}
		}
	}
	var addClient = func(c *lib.Client) int {
		mutex.Lock()
		defer mutex.Unlock()
		id := nextId
		nextId++
		joined[id] = c
		return id
	}
	var removeClient = func(id int) {
		mutex.Lock()
		defer mutex.Unlock()
		delete(joined, id)
	}
	var listClients = func() string {
		mutex.Lock()
		defer mutex.Unlock()
		var b strings.Builder
		first := true
		for _, c := range joined {
			if first {
				first = false
			} else {
				b.WriteString(", ")
			}
			b.WriteString(c.Log.Name)
		}
		return b.String()
	}

	lib.ServeTcp(func(c *lib.Client) (err error) {
		c.WriteLine("Welcome to budgetchat! What shall I call you?")
		name, err := c.ReadLine()
		if err != nil {
			return err
		}
		if !isValidName(name) {
			c.Log.Warn("Invalid name: %q", name)
			return nil
		}
		c.Log.Name = name

		c.WriteLine("* The room contains: " + listClients())
		id := addClient(c)
		defer removeClient(id)
		c.Log.Debug("Joined")
		broadcast(id, "* %s has entered the room", name)
		defer broadcast(id, "* %s has left the room", name)

		for c.HasData() {
			msg, err := c.ReadLine()
			if err != nil {
				return err
			}
			c.Log.Debug("=> %q", msg)
			broadcast(id, "[%s] %s", name, msg)
		}
		return nil
	})
}
