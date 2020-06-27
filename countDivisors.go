package main

import (
	"fmt"
)

// B2i convert bool to int
func B2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func countDivisors(num int) int {
	ans := 1
	x := 2

	for x*x <= num {
		cnt := 1
		for num%x == 0 {
			cnt++
			num /= x
		}
		ans = ans * cnt
	}

	return ans * (1 + B2i(num > 1))
}

func main() {
	var x int
	fmt.Scanln(&x)
	res := countDivisors(x)
	fmt.Println(res)
}
