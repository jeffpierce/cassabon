package main

import (
	"fmt"
	"net"
)

func stats() {

	addr := net.UDPAddr{Port: 8125, IP: net.ParseIP("127.0.0.1")}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		if n, _, err := conn.ReadFromUDP(buf); err == nil {
			fmt.Println(string(buf[0:n]))
		} else {
			fmt.Println("Error: ", err)
		}
	}
}
