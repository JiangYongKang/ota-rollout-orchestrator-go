// Command otactl is a small CLI client for the otad HTTP API.
//
// Usage examples:
//
//	otactl device add dev-1 v1.0
//	otactl devices
//	otactl create r1 v2.0 --devices dev-1,dev-2 --sizes 1,1 [--ratio 0.8]
//	otactl list
//	otactl get r1
//	otactl participants r1
//	otactl events r1
//	otactl pause r1 | resume r1 | terminate r1
//	otactl report dev-1 r1 --seq 1 --from v1.0 --current v2.0 [--fail]
//	otactl delete-device dev-1
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	base := envOr("OTAD_ADDR", "http://127.0.0.1:8080")
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "device":
		if len(args) >= 1 && args[0] == "add" {
			err = requireN(args, 3, "device add <id> <version>")
			if err == nil {
				err = post(base, "/v1/devices", map[string]string{"id": args[1], "version": args[2]}, http.StatusCreated)
			}
		} else {
			err = fmt.Errorf("usage: otactl device add <id> <version>")
		}
	case "devices":
		err = getPrint(base, "/v1/devices")
	case "offline", "online":
		on := cmd == "online"
		err = requireN(args, 1, cmd+" <device-id>")
		if err == nil {
			err = post(base, "/v1/devices/"+args[0]+"/simulate",
				map[string]bool{"online": on}, http.StatusOK)
		}
	case "fault":
		err = requireN(args, 2, "fault <device-id> <INSTALL_FAIL|DROP_COMMAND|DROP_REPORT|NO_OP|NONE>")
		if err == nil {
			fault := args[1]
			if fault == "NONE" {
				fault = ""
			}
			err = post(base, "/v1/devices/"+args[0]+"/simulate",
				map[string]string{"fault": fault}, http.StatusOK)
		}
	case "delete-device":
		err = requireN(args, 1, "delete-device <id>")
		if err == nil {
			err = doStatus(http.MethodDelete, base+"/v1/devices/"+args[0], nil, http.StatusNoContent)
		}
	case "create":
		err = createRollout(base, args)
	case "list":
		err = getPrint(base, "/v1/rollouts")
	case "get":
		err = requireN(args, 1, "get <rollout-id>")
		if err == nil {
			err = getPrint(base, "/v1/rollouts/"+args[0])
		}
	case "participants":
		err = requireN(args, 1, "participants <rollout-id>")
		if err == nil {
			err = getPrint(base, "/v1/rollouts/"+args[0]+"/participants")
		}
	case "events":
		err = requireN(args, 1, "events <rollout-id>")
		if err == nil {
			err = getPrint(base, "/v1/rollouts/"+args[0]+"/events")
		}
	case "pause", "resume", "terminate":
		err = requireN(args, 1, cmd+" <rollout-id>")
		if err == nil {
			err = post(base, "/v1/rollouts/"+args[0]+"/"+cmd, nil, http.StatusOK)
		}
	case "report":
		err = reportDevice(base, args)
	case "watch":
		err = watch(base, args)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func createRollout(base string, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: otactl create <id> <target-version> --devices a,b --sizes 2,3 [--ratio 0.8]")
	}
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	devices := fs.String("devices", "", "comma-separated device IDs")
	sizesStr := fs.String("sizes", "", "comma-separated batch sizes")
	ratio := fs.Float64("ratio", 0, "success ratio (0,1]")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	if *devices == "" || *sizesStr == "" {
		return fmt.Errorf("--devices and --sizes are required")
	}
	body := map[string]any{
		"id":             args[0],
		"target_version": args[1],
		"device_ids":     strings.Split(*devices, ","),
		"batch_sizes":    parseIntList(*sizesStr),
	}
	if *ratio > 0 {
		body["success_ratio"] = *ratio
	}
	return post(base, "/v1/rollouts", body, http.StatusCreated)
}

func reportDevice(base string, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: otactl report <device-id> <rollout-id> --seq N --from v --current v [--fail]")
	}
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	seq := fs.Int64("seq", 1, "command/report sequence number")
	from := fs.String("from", "", "version upgraded from")
	current := fs.String("current", "", "current version after the operation")
	fail := fs.Bool("fail", false, "report failure")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	body := map[string]any{
		"device_id":       args[0],
		"rollout_id":      args[1],
		"seq":             *seq,
		"from_version":    *from,
		"current_version": *current,
		"install_ok":      !*fail,
		"online":          true,
	}
	return post(base, "/v1/reports", body, http.StatusOK)
}

func watch(base string, args []string) error {
	if err := requireN(args, 1, "watch <rollout-id>"); err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		var r map[string]any
		if err := getJSON(base, "/v1/rollouts/"+args[0], &r); err != nil {
			return err
		}
		phase, _ := r["phase"].(string)
		if phase != last {
			fmt.Printf("phase=%s reason=%v\n", phase, r["reason"])
			last = phase
		}
		if phase == "COMPLETED" || phase == "FAILED" || phase == "TERMINATED" {
			return getPrint(base, "/v1/rollouts/"+args[0])
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("watch timed out")
}

func parseIntList(s string) []int {
	var out []int
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err == nil {
			out = append(out, n)
		}
	}
	return out
}

func requireN(args []string, n int, syntax string) error {
	if len(args) != n {
		return fmt.Errorf("usage: otactl %s", syntax)
	}
	return nil
}

func post(base, path string, body any, want int) error {
	var r io.Reader
	if body != nil {
		data, err := json.MarshalIndent(body, "", "  ")
		if err != nil {
			return err
		}
		r = bytes.NewReader(data)
	}
	req, err := http.NewRequest(http.MethodPost, base+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return do(req, want)
}

func doStatus(method, url string, body any, want int) error {
	var r io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		r = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return err
	}
	return do(req, want)
}

func getPrint(base, path string) error {
	resp, err := http.Get(base + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return printJSON(data)
}

func getJSON(base, path string, out any) error {
	resp, err := http.Get(base + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return json.Unmarshal(data, out)
}

func do(req *http.Request, want int) error {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		return fmt.Errorf("HTTP %d (want %d): %s", resp.StatusCode, want, strings.TrimSpace(string(data)))
	}
	if len(data) > 0 {
		return printJSON(data)
	}
	return nil
}

func printJSON(data []byte) error {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		fmt.Println(string(data))
		return nil
	}
	pretty, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(pretty))
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func usage() {
	fmt.Fprintln(os.Stderr, strings.TrimSpace(`
otactl - local OTA rollout orchestrator client

  device add <id> <version>          register a simulated device
  devices                            list devices
  offline <id> | online <id>         take device down / reconnect it
  fault <id> <kind>                  inject INSTALL_FAIL|DROP_COMMAND|DROP_REPORT|NO_OP|NONE
  delete-device <id>                 remove a device
  create <id> <target> \
         --devices a,b --sizes 2,3 [--ratio 0.8]
  list                               list rollouts
  get <id>                           rollout detail (batches/gate counters)
  participants <id>                  per-device state and reasons
  events <id>                        audit event trail
  pause <id> | resume <id> | terminate <id>
  report <device> <rollout> --seq N --from v --current v [--fail]
  watch <id>                         follow phase transitions

Set OTAD_ADDR to override http://127.0.0.1:8080`))
}
