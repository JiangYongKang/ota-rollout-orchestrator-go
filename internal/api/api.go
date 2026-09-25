// Package api exposes the orchestrator over a small local HTTP API.
//
// Endpoints:
//
//	POST   /v1/devices                 {"id","version"}
//	GET    /v1/devices
//	DELETE /v1/devices/{id}
//	POST   /v1/reports                 device report ingress
//	POST   /v1/rollouts                create rollout
//	GET    /v1/rollouts
//	GET    /v1/rollouts/{id}
//	POST   /v1/rollouts/{id}/pause
//	POST   /v1/rollouts/{id}/resume
//	POST   /v1/rollouts/{id}/terminate
//	GET    /v1/rollouts/{id}/participants
//	GET    /v1/rollouts/{id}/events
//
// The report endpoint is what a device gateway (here: the simulated fleet)
// calls; management endpoints are what operators/otactl call.
package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/fleet"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/orchestrator"
)

type Server struct {
	o orchestrator.Orchestrator
}

func NewServer(o orchestrator.Orchestrator) http.Handler {
	s := &Server{o: o}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/devices", s.createDevice)
	mux.HandleFunc("GET /v1/devices", s.listDevices)
	mux.HandleFunc("DELETE /v1/devices/{id}", s.deleteDevice)
	mux.HandleFunc("POST /v1/devices/{id}/simulate", s.simulateDevice)
	mux.HandleFunc("POST /v1/reports", s.report)
	mux.HandleFunc("POST /v1/rollouts", s.createRollout)
	mux.HandleFunc("GET /v1/rollouts", s.listRollouts)
	mux.HandleFunc("GET /v1/rollouts/{id}", s.getRollout)
	mux.HandleFunc("POST /v1/rollouts/{id}/pause", s.pause)
	mux.HandleFunc("POST /v1/rollouts/{id}/resume", s.resume)
	mux.HandleFunc("POST /v1/rollouts/{id}/terminate", s.terminate)
	mux.HandleFunc("GET /v1/rollouts/{id}/participants", s.participants)
	mux.HandleFunc("GET /v1/rollouts/{id}/events", s.events)
	mux.HandleFunc("GET /v1/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

type deviceReq struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

func (s *Server) createDevice(w http.ResponseWriter, r *http.Request) {
	var req deviceReq
	if !decode(w, r, &req) {
		return
	}
	d, err := s.o.UpsertDevice(r.Context(), req.ID, req.Version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, d)
}

func (s *Server) listDevices(w http.ResponseWriter, r *http.Request) {
	ds, err := s.o.Devices(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ds)
}

func (s *Server) deleteDevice(w http.ResponseWriter, r *http.Request) {
	if err := s.o.RemoveDevice(r.Context(), r.PathValue("id")); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type simulateReq struct {
	Online *bool       `json:"online,omitempty"`
	Fault  fleet.Fault `json:"fault,omitempty"`
}

// simulateDevice drives the simulated device: offline/reconnect and faults
// (INSTALL_FAIL / DROP_COMMAND / DROP_REPORT / NO_OP). It is a simulation-only
// control surface, not a production device operation.
func (s *Server) simulateDevice(w http.ResponseWriter, r *http.Request) {
	var req simulateReq
	if r.ContentLength > 0 {
		if !decode(w, r, &req) {
			return
		}
	}
	id := r.PathValue("id")
	if req.Online != nil {
		if err := s.o.SetDeviceOnline(r.Context(), id, *req.Online); err != nil {
			writeError(w, err)
			return
		}
	}
	if req.Fault != "" {
		if err := s.o.SetDeviceFault(r.Context(), id, req.Fault); err != nil {
			writeError(w, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "applied", "device": id})
}

func (s *Server) report(w http.ResponseWriter, r *http.Request) {
	var rep domain.Report
	if !decode(w, r, &rep) {
		return
	}
	ack, err := s.o.HandleReport(r.Context(), rep)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ack)
}

func (s *Server) createRollout(w http.ResponseWriter, r *http.Request) {
	var req orchestrator.CreateRequest
	if !decode(w, r, &req) {
		return
	}
	roll, err := s.o.CreateRollout(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, roll)
}

func (s *Server) listRollouts(w http.ResponseWriter, r *http.Request) {
	rs, err := s.o.Rollouts(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rs)
}

func (s *Server) getRollout(w http.ResponseWriter, r *http.Request) {
	roll, err := s.o.Rollout(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, roll)
}

func (s *Server) participants(w http.ResponseWriter, r *http.Request) {
	ps, err := s.o.Participants(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ps)
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	evs, err := s.o.Events(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, evs)
}

func (s *Server) pause(w http.ResponseWriter, r *http.Request) {
	roll, err := s.o.PauseRollout(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, roll)
}

func (s *Server) resume(w http.ResponseWriter, r *http.Request) {
	roll, err := s.o.ResumeRollout(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, roll)
}

func (s *Server) terminate(w http.ResponseWriter, r *http.Request) {
	roll, err := s.o.TerminateRollout(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, roll)
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, domain.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, domain.ErrInvalidArgument):
		status = http.StatusBadRequest
	case errors.Is(err, domain.ErrAlreadyExists),
		errors.Is(err, domain.ErrConflict),
		errors.Is(err, domain.ErrDeviceBusy):
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
