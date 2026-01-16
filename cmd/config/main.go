package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/patrickbucher/meow"
	"github.com/valkey-io/valkey-go"
)

func main() {
	addrFlag := flag.String("addr", "0.0.0.0", "listen to address")
	port := flag.Uint("port", 8000, "listen on port")
	flag.Parse()

	log.SetOutput(os.Stderr)

	rawValkeyURL, ok := os.LookupEnv("VALKEY_URL")
	if !ok || strings.TrimSpace(rawValkeyURL) == "" {
		log.Fatalf("environment variable VALKEY_URL must be set (example: valkey.frickelcloud.ch:6379/4)")
	}

	valkeyAddr, valkeyDB, err := parseValkeyURL(rawValkeyURL)
	if err != nil {
		log.Fatalf("parse VALKEY_URL %q: %v", rawValkeyURL, err)
	}

	ctx := context.Background()

	options := valkey.ClientOption{
		InitAddress: []string{valkeyAddr},
		SelectDB:    valkeyDB,
	}
	vk, err := valkey.NewClient(options)
	if err != nil {
		log.Fatalf("connect to valkey at %s (db %d): %v", valkeyAddr, valkeyDB, err)
	}

	// quick connectivity check
	if err := vk.Do(ctx, vk.B().Set().Key("purpose").Value("meow").Build()).Error(); err != nil {
		log.Fatalf("valkey SET purpose=meow failed: %v", err)
	}

	http.HandleFunc("/endpoints/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getEndpoint(ctx, vk, w, r)
		case http.MethodPost:
			postEndpoint(ctx, vk, w, r)
		// TODO: support http.MethodDelete to delete endpoints (optional task)
		default:
			log.Printf("request from %s rejected: method %s not allowed",
				r.RemoteAddr, r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	http.HandleFunc("/endpoints", func(w http.ResponseWriter, r *http.Request) {
		getEndpoints(ctx, vk, w, r)
	})

	listenTo := fmt.Sprintf("%s:%d", *addrFlag, *port)
	log.Printf("listen to %s (valkey=%s db=%d)", listenTo, valkeyAddr, valkeyDB)
	http.ListenAndServe(listenTo, nil)
}

func parseValkeyURL(raw string) (addr string, db int, err error) {
	raw = strings.TrimSpace(raw)

	// Strip scheme if present
	raw = strings.TrimPrefix(raw, "redis://")
	raw = strings.TrimPrefix(raw, "valkey://")

	db = 0

	// Extract db from trailing /<number>
	if i := strings.LastIndex(raw, "/"); i >= 0 {
		dbStr := raw[i+1:]
		raw = raw[:i]
		if dbStr != "" {
			db, err = strconv.Atoi(dbStr)
			if err != nil {
				return "", 0, fmt.Errorf("invalid DB number in VALKEY_URL: %v", err)
			}
		}
	}

	// Ensure host:port
	if _, _, splitErr := net.SplitHostPort(raw); splitErr != nil {
		raw = raw + ":6379"
	}

	return raw, db, nil
}

func endpointKey(identifier string) string {
	return fmt.Sprintf("endpoints:%s", identifier)
}

func getEndpoint(ctx context.Context, vk valkey.Client, w http.ResponseWriter, r *http.Request) {
	log.Printf("GET %s from %s", r.URL, r.RemoteAddr)

	identifier, err := extractEndpointIdentifier(r.URL.String())
	if err != nil {
		log.Printf("extract endpoint identifier of %s: %v", r.URL, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	key := endpointKey(identifier)
	kvs, err := vk.Do(ctx, vk.B().Hgetall().Key(key).Build()).AsStrMap()
	if err != nil {
		log.Printf("hgetall %s: %v", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if len(kvs) == 0 {
		log.Printf(`no such endpoint "%s"`, identifier)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	payload, err := payloadFromValkeyMap(kvs)
	if err != nil {
		log.Printf("convert valkey hash %s to payload: %v", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("marshal payload to JSON: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(data)
}

func postEndpoint(ctx context.Context, vk valkey.Client, w http.ResponseWriter, r *http.Request) {
	log.Printf("POST %s from %s", r.URL, r.RemoteAddr)

	identifierPathParam, err := extractEndpointIdentifier(r.URL.String())
	if err != nil {
		log.Printf("extract endpoint identifier of %s: %v", r.URL, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	buf := bytes.NewBufferString("")
	_, _ = io.Copy(buf, r.Body)
	defer r.Body.Close()

	endpoint, err := meow.EndpointFromJSON(buf.String())
	if err != nil {
		log.Printf("parse JSON body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Must match, otherwise reject
	if identifierPathParam != endpoint.Identifier {
		log.Printf("identifier mismatch: (resource: %s, body: %s)",
			identifierPathParam, endpoint.Identifier)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	key := endpointKey(endpoint.Identifier)

	// existence check for correct status code
	existing, err := vk.Do(ctx, vk.B().Hgetall().Key(key).Build()).AsStrMap()
	if err != nil {
		log.Printf("hgetall %s (exists check): %v", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	exists := len(existing) > 0

	// store endpoint as hash (all fields as strings)
	cmd := vk.B().Hset().Key(key).
		FieldValue().
		FieldValue("identifier", endpoint.Identifier).
		FieldValue("url", endpoint.URL.String()).
		FieldValue("method", endpoint.Method).
		FieldValue("status_online", strconv.Itoa(int(endpoint.StatusOnline))).
		FieldValue("frequency", endpoint.Frequency.String()).
		FieldValue("fail_after", strconv.Itoa(int(endpoint.FailAfter))).
		Build()

	if err := vk.Do(ctx, cmd).Error(); err != nil {
		log.Printf("hset %s: %v", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if exists {
		w.WriteHeader(http.StatusNoContent) // updated
	} else {
		w.WriteHeader(http.StatusCreated) // created
	}
}

func getEndpoints(ctx context.Context, vk valkey.Client, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		log.Printf("request from %s rejected: method %s not allowed",
			r.RemoteAddr, r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	log.Printf("GET %s from %s", r.URL, r.RemoteAddr)

	keys, err := vk.Do(ctx, vk.B().Keys().Pattern("endpoints:*").Build()).AsStrSlice()
	if err != nil {
		log.Printf("get keys for endpoints:*: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	payloads := make([]meow.EndpointPayload, 0)
	for _, key := range keys {
		kvs, err := vk.Do(ctx, vk.B().Hgetall().Key(key).Build()).AsStrMap()
		if err != nil {
			log.Printf("hgetall %s: %v", key, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if len(kvs) == 0 {
			continue
		}
		payload, err := payloadFromValkeyMap(kvs)
		if err != nil {
			log.Printf("convert valkey hash %s to payload: %v", key, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		payloads = append(payloads, payload)
	}

	data, err := json.Marshal(payloads)
	if err != nil {
		log.Printf("serialize payloads: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

func payloadFromValkeyMap(kvs map[string]string) (meow.EndpointPayload, error) {
	id := kvs["identifier"]
	url := kvs["url"]
	method := kvs["method"]
	freq := kvs["frequency"]

	statusStr := kvs["status_online"]
	failStr := kvs["fail_after"]

	if id == "" || url == "" || method == "" || freq == "" || statusStr == "" || failStr == "" {
		return meow.EndpointPayload{}, fmt.Errorf("missing fields in valkey hash: %v", kvs)
	}

	statusInt, err := strconv.Atoi(statusStr)
	if err != nil {
		return meow.EndpointPayload{}, fmt.Errorf("status_online not a number: %q: %v", statusStr, err)
	}
	failInt, err := strconv.Atoi(failStr)
	if err != nil {
		return meow.EndpointPayload{}, fmt.Errorf("fail_after not a number: %q: %v", failStr, err)
	}

	return meow.EndpointPayload{
		Identifier:   id,
		URL:          url,
		Method:       method,
		StatusOnline: uint16(statusInt),
		Frequency:    freq,
		FailAfter:    uint8(failInt),
	}, nil
}

const endpointIdentifierPatternRaw = "^/endpoints/([a-z][-a-z0-9]+)$"

var endpointIdentifierPattern = regexp.MustCompile(endpointIdentifierPatternRaw)

func extractEndpointIdentifier(endpoint string) (string, error) {
	matches := endpointIdentifierPattern.FindStringSubmatch(endpoint)
	if len(matches) == 0 {
		return "", fmt.Errorf(`endpoint "%s" does not match pattern "%s"`,
			endpoint, endpointIdentifierPatternRaw)
	}
	return matches[1], nil
}
