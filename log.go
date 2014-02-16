package genmai

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"text/template"
	"time"
)

const defaultLoggingFormat = `[{{.time.Format "2006-01-02 15:04:05"}}] [{{.duration}}] {{.query}}`

var (
	defaultLoggerTemplate = template.Must(template.New("genmai").Parse(defaultLoggingFormat))
	defaultLogger         = &nullLogger{}
)

// logger is the interface that query logger.
type logger interface {
	// Print outputs query log.
	Print(start time.Time, query string, args ...interface{}) error

	// SetFormat sets the format for logging.
	SetFormat(format string) error
}

// templateLogger is a logger that Go's template to be used as a format.
// It implements the logger interface.
type templateLogger struct {
	w io.Writer
	t *template.Template
	m sync.Mutex
}

// SetFormat sets the format for logging.
func (l *templateLogger) SetFormat(format string) error {
	l.m.Lock()
	defer l.m.Unlock()
	t, err := template.New("genmai").Parse(format)
	if err != nil {
		return err
	}
	l.t = t
	return nil
}

// Print outputs query log using format template.
// All arguments will be used to formatting.
func (l *templateLogger) Print(start time.Time, query string, args ...interface{}) error {
	if len(args) > 0 {
		values := make([]string, len(args))
		for i, arg := range args {
			values[i] = fmt.Sprintf("%#v", arg)
		}
		query = fmt.Sprintf("%v; [%v]", query, strings.Join(values, ", "))
	} else {
		query = fmt.Sprintf("%s;", query)
	}
	data := map[string]interface{}{
		"time":     start,
		"duration": fmt.Sprintf("%.2fms", now().Sub(start).Seconds()*float64(time.Microsecond)),
		"query":    query,
	}
	var buf bytes.Buffer
	if err := l.t.Execute(&buf, data); err != nil {
		return err
	}
	l.m.Lock()
	defer l.m.Unlock()
	if _, err := fmt.Fprintln(l.w, strings.TrimSuffix(buf.String(), "\n")); err != nil {
		return err
	}
	return nil
}

// nullLogger is a null logger.
// It implements the logger interface.
type nullLogger struct{}

// SetFormat is a dummy method.
func (l *nullLogger) SetFormat(format string) error {
	return nil
}

// Print is a dummy method.
func (l *nullLogger) Print(start time.Time, query string, args ...interface{}) error {
	return nil
}
