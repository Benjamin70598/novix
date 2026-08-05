module github.com/Benjamin70598/novix

go 1.22

require (
	github.com/mattn/go-sqlite3 v1.14.22
	gopkg.in/yaml.v3 v3.0.1
)

replace gopkg.in/yaml.v3 => ./third_party/yaml.v3
