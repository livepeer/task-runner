package api

import (
	"errors"
	"net/http"
	"strings"
)

func authorized(authSecret string, next http.Handler) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			respondError(rw, http.StatusUnauthorized, errors.New("no authorization header provided"))
			return
		}
		authFields := strings.Fields(authHeader)
		if len(authFields) != 2 || strings.ToLower(authFields[0]) != "bearer" {
			respondError(rw, http.StatusUnauthorized, errors.New("invalid authorization header. Must have format 'Bearer <secret>'"))
			return
		}
		token := authFields[1]
		if token != authSecret {
			respondError(rw, http.StatusForbidden, errors.New("invalid authorization secret"))
			return
		}
		next.ServeHTTP(rw, r)
	}
}
