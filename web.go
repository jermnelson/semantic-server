/*
Package web implements a web-based management tool for the Semantic
Server project.

Author = Jeremy Nelson
License = GPLv3
 */
package main

import (
    "html/template"
    "fmt"
    "net/http"
)

func containerHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Container Handler %s", r.URL.Path)
}

func handler(w http.ResponseWriter, r *http.Request) {
    t, _ := template.ParseFiles("index.html")
    t.Execute(w, nil)
}

func main() {
    fmt.Printf("Running on port 7899")
    http.HandleFunc("/", handler)
    http.Handle("/images/", http.StripPrefix("/images/", http.FileServer(http.Dir("images"))))
    http.ListenAndServe(":7899", nil)
}
