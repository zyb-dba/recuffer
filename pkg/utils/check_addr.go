package utils

import (
	"net"
	"strconv"
	"strings"
)

func IsNum(s string) bool {
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

func CheckAddr(addr string) bool {
	if strings.Count(addr, ":") == 1 {
		addr_slic := strings.Split(addr, ":")
		if len(addr_slic) == 2 {
			if net.ParseIP(addr_slic[0]) != nil && IsNum(addr_slic[1]) {
				return true
			} else {
				return false
			}
		} else {
			return false
		}
	} else {
		return false
	}
}
