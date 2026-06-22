# Code Style and Conventions

## Code Style Configuration
- **Import**: Use `hugegraph-style.xml` in your IDE (IntelliJ IDEA recommended)
- **EditorConfig**: `.editorconfig` file defines style rules (validated in CI)
- **Checkstyle**: `style/checkstyle.xml` defines additional rules

## Core Style Rules (from .editorconfig)

### General
- Charset: UTF-8
- End of line: LF (Unix-style)
- Insert final newline: true
- Max line length: 100 characters (120 for XML)
- Visual guides at column 100

### Java Files
- Indent: 4 spaces (not tabs)
- Continuation indent: 8 spaces
- Wrap on typing: true
- Wrap long lines: true

### Import Organization
```
$*
|
java.**
|
javax.**
|
org.**
|
com.**
|
*
```
- Class count to use import on demand: 100
- Names count to use import on demand: 100

### Formatting Rules
- Line comments not at first column
- Align multiline: chained methods, parameters in calls, binary operations, assignments, ternary, throws, extends, array initializers
- Wrapping: normal (wrap if necessary)
- Brace forcing:
  - if: if_multiline
  - do-while: always
  - while: if_multiline
  - for: if_multiline
- Enum constants: split_into_lines

### Blank Lines
- Max blank lines in declarations: 1
- Max blank lines in code: 1
- Blank lines between package declaration and header: 1
- Blank lines before right brace: 1
- Blank lines around class: 1
- Blank lines after class header: 1

### Documentation
- Add `<p>` tag on empty lines: true
- Do not wrap if one line: true
- Align multiline annotation parameters: true

### XML Files
- Indent: 4 spaces
- Max line length: 120
- Text wrap: off
- Space inside empty tag: true

### Maven
- Compiler source/target: Java 11
- Max compiler errors: 500
- Compiler args: `-Xlint:unchecked`
- Source encoding: UTF-8

## Lombok Usage
- Version: 1.18.30
- Scope: provided
- Optional: true

## License Headers
- All source files MUST include Apache Software License header
- Validated by apache-rat-plugin and skywalking-eyes
- Exclusions defined in pom.xml (line 171-221)
- gRPC generated code excluded from license check

## Naming Conventions
- Package names: lowercase, dot-separated (e.g., org.apache.hugegraph)
- Class names: PascalCase
- Method names: camelCase
- Constants: UPPER_SNAKE_CASE
- Variables: camelCase
