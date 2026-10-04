package web

import (
	"errors"
	"net/http"
	"time"

	"github.com/MikelGV/PierceMQ/internal/web/views"
)

// sessionTTL mirrors the API JWT TTL for the cookie lifetime.
const sessionTTL = 24 * time.Hour

// requireSession redirects anonymous browsers to login. Expiry mid-session
// surfaces as API 401 inside handlers, which also funnel here.
func (s *Server) requireSession(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := tokenFromRequest(r); !ok {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		next(w, r)
	}
}

// redirectLogin clears a dead session and sends the browser to login.
func redirectLogin(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: "", Path: "/",
		MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// htmxRedirect tells an htmx fragment swap to navigate full-page instead
// (a 302 inside hx-get would swap the login form into a table div).
func htmxRedirect(w http.ResponseWriter, to string) {
	w.Header().Set("HX-Redirect", to)
	w.WriteHeader(http.StatusNoContent)
}

func setSession(w http.ResponseWriter, token string) {
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: token, Path: "/",
		MaxAge: int(sessionTTL.Seconds()), HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}

func (s *Server) routeLogin(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if _, ok := tokenFromRequest(r); ok {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
		_ = views.Login("", "").Render(r.Context(), w)
	case http.MethodPost:
		s.handleLoginPost(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleLoginPost(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		_ = views.Login("", "invalid form").Render(r.Context(), w)
		return
	}
	email, password := r.FormValue("email"), r.FormValue("password")
	var out map[string]string
	_, err := s.apiDo(r, http.MethodPost, "/v1/auth/login",
		map[string]string{"email": email, "password": password}, &out)
	if err != nil {
		if apiStatusOf(err) == http.StatusUnauthorized {
			w.WriteHeader(http.StatusUnauthorized)
			_ = views.Login(email, "invalid credentials").Render(r.Context(), w)
			return
		}
		w.WriteHeader(http.StatusBadGateway)
		_ = views.Login(email, "login failed: "+err.Error()).Render(r.Context(), w)
		return
	}
	setSession(w, out["token"])
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) routeRegister(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		_ = views.Register("", "", "").Render(r.Context(), w)
	case http.MethodPost:
		s.handleRegisterPost(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleRegisterPost(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		_ = views.Register("", "", "invalid form").Render(r.Context(), w)
		return
	}
	name, email, password := r.FormValue("name"), r.FormValue("email"), r.FormValue("password")
	var regOut map[string]string
	if _, err := s.apiDo(r, http.MethodPost, "/v1/auth/register",
		map[string]string{"name": name, "email": email, "password": password}, &regOut); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		msg := "registration failed"
		var ae *apiError
		if errors.As(err, &ae) && ae.Msg != "" {
			msg = ae.Msg
		}
		_ = views.Register(name, email, msg).Render(r.Context(), w)
		return
	}
	// Auto-login on success.
	var loginOut map[string]string
	if _, err := s.apiDo(r, http.MethodPost, "/v1/auth/login",
		map[string]string{"email": email, "password": password}, &loginOut); err != nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	setSession(w, loginOut["token"])
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	redirectLogin(w, r)
}
