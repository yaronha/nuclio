package main

import (
	"html/template"
	"bytes"
	"fmt"
)

type user struct {
	Name string
}

func main() {
	t := template.New("login")
	t, _ = t.Parse("hello {{.}}!")
	//u := user{Name:"yaron"}
	var tpl bytes.Buffer
	t.Execute(&tpl, "joe")

	fmt.Println(tpl.String())

}