package main

import (
	"bufio"
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v4"
)

func main() {
	// Read in connection string
	scanner := bufio.NewScanner(os.Stdin)
	log.Println("Enter a connection string: ")
	scanner.Scan()
	connstring := scanner.Text()

	// Attempt to connect
	config, err := pgx.ParseConfig(os.ExpandEnv(connstring))
	if err != nil {
		log.Fatal("error configuring the database: ", err)
	}
	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatal("error connecting to the database: ", err)
	}
	defer conn.Close(context.Background())
	log.Println("Hey! You successfully connected to your CockroachDB cluster.")

}
