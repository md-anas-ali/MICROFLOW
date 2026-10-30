package api

import (
	"errors"
	"net/http"

	"microflow/internal/localmodel"
)

// LocalModelController is the slice of localmodel.Manager the API exposes
// (Settings -> Model). Everything runs on the machine MicroFlow runs on.
type LocalModelController interface {
	Status() localmodel.Status
	Install() error
	CancelInstall()
	Uninstall() error
	Start() error
	Stop() error
	SetEnabled(on bool) error
}

// WithLocalModel registers /api/local-model/*. Nil-safe: without it no routes
// exist and the rest of the server is unchanged.
func (s *Server) WithLocalModel(c LocalModelController) *Server {
	if c == nil {
		return s
	}
	s.localModel = c
	s.mux.HandleFunc("GET /api/local-model/status", s.handleLocalModelStatus)
	s.mux.HandleFunc("POST /api/local-model/{action}", s.handleLocalModelAction)
	return s
}

func (s *Server) handleLocalModelStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.localModel.Status())
}

func (s *Server) handleLocalModelAction(w http.ResponseWriter, r *http.Request) {
	c := s.localModel
	var err error
	switch r.PathValue("action") {
	case "install":
		err = c.Install()
	case "cancel-install":
		c.CancelInstall()
	case "uninstall":
		err = c.Uninstall()
	case "start":
		err = c.Start()
	case "stop":
		err = c.Stop()
	case "enable":
		err = c.SetEnabled(true)
	case "disable":
		err = c.SetEnabled(false)
	default:
		writeErr(w, http.StatusNotFound, errors.New("unknown action"))
		return
	}
	if err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, c.Status())
}
