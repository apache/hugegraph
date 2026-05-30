# Code Style and Conventions

## Style Tools
- `.editorconfig` — Primary style definition
- `style/checkstyle.xml` — Enforcement (note: `hugegraph-style.xml` was removed)
- `.licenserc.yaml` + apache-rat-plugin + skywalking-eyes — License header validation

## Core Rules
- **Line length**: 100 chars (120 for XML)
- **Indent**: 4 spaces, continuation 8 spaces
- **Charset**: UTF-8, LF line endings, final newline
- **Imports**: Sorted `$*` → `java` → `javax` → `org` → `com` → `*`, no star imports (threshold 100)
- **Braces**: `if`/`while`/`for` forced if multiline, `do-while` always
- **Blank lines**: Max 1 in code/declarations
- **JavaDoc**: `<p>` on empty lines, no wrap if one line

## Naming
- Packages: `org.apache.hugegraph.*` (lowercase dot-separated)
- Classes: PascalCase, Methods/Variables: camelCase, Constants: UPPER_SNAKE_CASE

## License
- All source files require Apache 2.0 header
- gRPC generated code excluded from checks
- Validate: `mvn apache-rat:check -ntp` + `mvn editorconfig:check`

## Build
- Java 11 target, `-Xlint:unchecked`, Lombok 1.18.30 (provided/optional)
- Swagger: `io.swagger.core.v3:swagger-jaxrs2-jakarta` for REST API docs
