package executor

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/correodabid/asql/internal/engine/parser"
	"github.com/correodabid/asql/internal/engine/parser/ast"
)

func compareLiteralByOperator(left ast.Literal, operator string, right ast.Literal) bool {
	if operator == "IS NULL" {
		return left.Kind == ast.LiteralNull
	}

	if operator == "IS NOT NULL" {
		return left.Kind != ast.LiteralNull
	}

	if left.Kind == ast.LiteralNull || right.Kind == ast.LiteralNull {
		return false
	}

	cmp := compareLiterals(left, right)
	switch operator {
	case "=":
		return cmp == 0
	case ">":
		return cmp > 0
	case "<":
		return cmp < 0
	case ">=":
		return cmp >= 0
	case "<=":
		return cmp <= 0
	default:
		return false
	}
}

func isSimplePredicate(predicate *ast.Predicate) bool {
	if predicate == nil {
		return true
	}

	if predicate.Left != nil || predicate.Right != nil {
		return false
	}

	switch strings.ToUpper(predicate.Operator) {
	case "AND", "OR", "NOT":
		return false
	default:
		return isSimpleColumnReference(predicate.Column)
	}
}

func isSimpleColumnReference(column string) bool {
	trimmed := strings.TrimSpace(column)
	if trimmed == "" {
		return false
	}
	return !strings.ContainsAny(trimmed, " +-*/()")
}

func evaluatePredicate3VL(row map[string]ast.Literal, predicate *ast.Predicate, state *readableState, engine *Engine) ternaryResult {
	if predicate == nil {
		return ternaryTrue
	}

	operator := strings.ToUpper(strings.TrimSpace(predicate.Operator))
	switch operator {
	case "NOT":
		left := evaluatePredicate3VL(row, predicate.Left, state, engine)
		switch left {
		case ternaryTrue:
			return ternaryFalse
		case ternaryFalse:
			return ternaryTrue
		default:
			return ternaryUnknown
		}
	case "AND":
		left := evaluatePredicate3VL(row, predicate.Left, state, engine)
		right := evaluatePredicate3VL(row, predicate.Right, state, engine)
		if left == ternaryFalse || right == ternaryFalse {
			return ternaryFalse
		}
		if left == ternaryUnknown || right == ternaryUnknown {
			return ternaryUnknown
		}
		return ternaryTrue
	case "OR":
		left := evaluatePredicate3VL(row, predicate.Left, state, engine)
		right := evaluatePredicate3VL(row, predicate.Right, state, engine)
		if left == ternaryTrue || right == ternaryTrue {
			return ternaryTrue
		}
		if left == ternaryUnknown || right == ternaryUnknown {
			return ternaryUnknown
		}
		return ternaryFalse
	case "EXISTS":
		if predicate.Subquery == nil {
			return ternaryFalse
		}
		return evaluateExistsSubquery(predicate.Subquery, state, engine, row)
	case "IN":
		if predicate.Subquery != nil {
			return evaluateInSubquery(row, predicate, state, engine, false)
		}
		if len(predicate.InValues) > 0 {
			return evaluateInList(row, predicate, false)
		}
	case "NOT IN":
		if predicate.Subquery != nil {
			return evaluateInSubquery(row, predicate, state, engine, true)
		}
		if len(predicate.InValues) > 0 {
			return evaluateInList(row, predicate, true)
		}
	case "BETWEEN":
		return evaluateBetween(row, predicate, false)
	case "NOT BETWEEN":
		return evaluateBetween(row, predicate, true)
	case "LIKE", "NOT LIKE":
		value, exists := resolvePredicateOperand(row, predicate.Column)
		if !exists {
			return ternaryFalse
		}
		if value.Kind == ast.LiteralNull {
			return ternaryUnknown
		}
		if predicate.Value.Kind == ast.LiteralNull {
			return ternaryUnknown
		}
		var strVal string
		switch value.Kind {
		case ast.LiteralString:
			strVal = value.StringValue
		default:
			return ternaryFalse
		}
		pattern := predicate.Value.StringValue
		matched := matchLikePattern(strVal, pattern)
		if operator == "NOT LIKE" {
			matched = !matched
		}
		if matched {
			return ternaryTrue
		}
		return ternaryFalse
	case "ILIKE", "NOT ILIKE":
		value, exists := resolvePredicateOperand(row, predicate.Column)
		if !exists {
			return ternaryFalse
		}
		if value.Kind == ast.LiteralNull {
			return ternaryUnknown
		}
		if predicate.Value.Kind == ast.LiteralNull {
			return ternaryUnknown
		}
		var strVal string
		switch value.Kind {
		case ast.LiteralString:
			strVal = value.StringValue
		default:
			return ternaryFalse
		}
		pattern := predicate.Value.StringValue
		matched := matchLikePattern(strings.ToLower(strVal), strings.ToLower(pattern))
		if operator == "NOT ILIKE" {
			matched = !matched
		}
		if matched {
			return ternaryTrue
		}
		return ternaryFalse
	}

	value, exists := resolvePredicateOperand(row, predicate.Column)
	if !exists && predicate.JsonAccess != nil {
		value, exists = resolveJsonAccess(row, predicate.JsonAccess)
	}
	if !exists {
		return ternaryFalse
	}

	if operator == "IS NULL" {
		if value.Kind == ast.LiteralNull {
			return ternaryTrue
		}
		return ternaryFalse
	}

	if operator == "IS NOT NULL" {
		if value.Kind != ast.LiteralNull {
			return ternaryTrue
		}
		return ternaryFalse
	}

	if value.Kind == ast.LiteralNull || predicate.Value.Kind == ast.LiteralNull {
		// Scalar subquery: col op (SELECT ...)
		if predicate.Subquery != nil && value.Kind != ast.LiteralNull {
			return evaluateScalarSubquery(value, operator, predicate.Subquery, state, engine, row)
		}
		// Column-to-column comparison where right side is null.
		if predicate.RightColumn != "" {
			rightVal, rightExists := resolvePredicateOperand(row, predicate.RightColumn)
			if !rightExists || rightVal.Kind == ast.LiteralNull {
				return ternaryUnknown
			}
			if value.Kind == ast.LiteralNull {
				return ternaryUnknown
			}
			if compareLiteralByOperator(value, operator, rightVal) {
				return ternaryTrue
			}
			return ternaryFalse
		}
		return ternaryUnknown
	}

	// Scalar subquery with non-null literal value
	if predicate.Subquery != nil {
		return evaluateScalarSubquery(value, operator, predicate.Subquery, state, engine, row)
	}

	// Column-to-column comparison (e.g., a.id = b.id, or correlated outer references).
	if predicate.RightColumn != "" {
		rightVal, rightExists := resolvePredicateOperand(row, predicate.RightColumn)
		if !rightExists {
			return ternaryFalse
		}
		if rightVal.Kind == ast.LiteralNull {
			return ternaryUnknown
		}
		if compareLiteralByOperator(value, operator, rightVal) {
			return ternaryTrue
		}
		return ternaryFalse
	}

	if compareLiteralByOperator(value, operator, predicate.Value) {
		return ternaryTrue
	}

	return ternaryFalse
}

func resolvePredicateOperand(row map[string]ast.Literal, expression string) (ast.Literal, bool) {
	trimmed := normalizeExpressionSyntax(expression)
	if trimmed == "" {
		return ast.Literal{}, false
	}

	// String literal: 'value'
	if len(trimmed) >= 2 && trimmed[0] == '\'' && trimmed[len(trimmed)-1] == '\'' {
		inner := trimmed[1 : len(trimmed)-1]
		return ast.Literal{Kind: ast.LiteralString, StringValue: inner}, true
	}

	canonical := strings.ToLower(trimmed)
	if value, exists := row[canonical]; exists {
		return value, true
	}

	if val, ok := evaluateCast(trimmed, row); ok {
		return val, true
	}

	// Row-level functions: COALESCE, NULLIF.
	if val, ok := evaluateRowFunction(canonical, row); ok {
		return val, true
	}

	value, err := evaluateNumericExpression(canonical, row)
	if err != nil {
		return ast.Literal{}, false
	}

	return value, true
}

func evaluateNumericExpression(expression string, row map[string]ast.Literal) (ast.Literal, error) {
	tokens, err := tokenizeArithmeticExpression(expression)
	if err != nil {
		return ast.Literal{}, err
	}
	if len(tokens) == 0 {
		return ast.Literal{}, fmt.Errorf("empty arithmetic expression")
	}

	output := make([]string, 0, len(tokens))
	operators := make([]string, 0)
	for _, token := range tokens {
		if isArithmeticOperator(token) {
			for len(operators) > 0 {
				top := operators[len(operators)-1]
				if !isArithmeticOperator(top) || arithmeticPrecedence(top) < arithmeticPrecedence(token) {
					break
				}
				output = append(output, top)
				operators = operators[:len(operators)-1]
			}
			operators = append(operators, token)
			continue
		}

		output = append(output, token)
	}

	for i := len(operators) - 1; i >= 0; i-- {
		output = append(output, operators[i])
	}

	stack := make([]ast.Literal, 0, len(output))
	for _, token := range output {
		if !isArithmeticOperator(token) {
			operand, err := resolveArithmeticToken(token, row)
			if err != nil {
				return ast.Literal{}, err
			}
			stack = append(stack, operand)
			continue
		}

		if len(stack) < 2 {
			return ast.Literal{}, fmt.Errorf("invalid arithmetic expression")
		}

		right := stack[len(stack)-1]
		left := stack[len(stack)-2]
		stack = stack[:len(stack)-2]

		result, err := applyArithmeticOperator(left, token, right)
		if err != nil {
			return ast.Literal{}, err
		}
		stack = append(stack, result)
	}

	if len(stack) != 1 {
		return ast.Literal{}, fmt.Errorf("invalid arithmetic expression")
	}

	return stack[0], nil
}

func tokenizeArithmeticExpression(expression string) ([]string, error) {
	tokens := make([]string, 0, 8)
	start := 0
	depth := 0
	inString := false
	for index := 0; index < len(expression); index++ {
		char := expression[index]
		if char == '\'' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		if char == '(' {
			depth++
			continue
		}
		if char == ')' {
			if depth > 0 {
				depth--
			}
			continue
		}

		if depth == 0 && (char == '+' || char == '-' || char == '*' || char == '/') {
			left := strings.TrimSpace(expression[start:index])
			if left == "" {
				return nil, fmt.Errorf("invalid arithmetic expression")
			}
			tokens = append(tokens, left)
			tokens = append(tokens, string(char))
			start = index + 1
		}
	}

	tail := strings.TrimSpace(expression[start:])
	if tail == "" {
		return nil, fmt.Errorf("invalid arithmetic expression")
	}
	tokens = append(tokens, tail)
	return tokens, nil
}

func isArithmeticOperator(token string) bool {
	switch token {
	case "+", "-", "*", "/":
		return true
	default:
		return false
	}
}

func arithmeticPrecedence(operator string) int {
	switch operator {
	case "*", "/":
		return 2
	case "+", "-":
		return 1
	default:
		return 0
	}
}

func resolveArithmeticToken(token string, row map[string]ast.Literal) (ast.Literal, error) {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return ast.Literal{}, fmt.Errorf("empty arithmetic token")
	}

	if number, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: number}, nil
	}

	if floatVal, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return ast.Literal{Kind: ast.LiteralFloat, FloatValue: floatVal}, nil
	}

	canonical := strings.ToLower(trimmed)
	value, exists := row[canonical]
	if !exists {
		return ast.Literal{}, fmt.Errorf("unknown arithmetic token %s", token)
	}

	if value.Kind == ast.LiteralNull {
		return ast.Literal{Kind: ast.LiteralNull}, nil
	}

	if value.Kind != ast.LiteralNumber && value.Kind != ast.LiteralFloat && value.Kind != ast.LiteralTimestamp {
		return ast.Literal{}, fmt.Errorf("non-numeric arithmetic token %s", token)
	}

	return value, nil
}

func applyArithmeticOperator(left ast.Literal, operator string, right ast.Literal) (ast.Literal, error) {
	if left.Kind == ast.LiteralNull || right.Kind == ast.LiteralNull {
		return ast.Literal{Kind: ast.LiteralNull}, nil
	}

	// Timestamp arithmetic: only +/- with integer offsets (microseconds)
	if left.Kind == ast.LiteralTimestamp || right.Kind == ast.LiteralTimestamp {
		if operator != "+" && operator != "-" {
			return ast.Literal{}, fmt.Errorf("timestamps only support + and - operators")
		}
		if left.Kind == ast.LiteralTimestamp && right.Kind == ast.LiteralNumber {
			switch operator {
			case "+":
				return ast.Literal{Kind: ast.LiteralTimestamp, NumberValue: left.NumberValue + right.NumberValue}, nil
			case "-":
				return ast.Literal{Kind: ast.LiteralTimestamp, NumberValue: left.NumberValue - right.NumberValue}, nil
			}
		}
		if left.Kind == ast.LiteralNumber && right.Kind == ast.LiteralTimestamp && operator == "+" {
			return ast.Literal{Kind: ast.LiteralTimestamp, NumberValue: left.NumberValue + right.NumberValue}, nil
		}
		if left.Kind == ast.LiteralTimestamp && right.Kind == ast.LiteralTimestamp && operator == "-" {
			return ast.Literal{Kind: ast.LiteralNumber, NumberValue: left.NumberValue - right.NumberValue}, nil
		}
		return ast.Literal{}, fmt.Errorf("unsupported timestamp arithmetic")
	}

	// Float arithmetic: promote if either operand is float
	if left.Kind == ast.LiteralFloat || right.Kind == ast.LiteralFloat {
		lf := left.FloatValue
		rf := right.FloatValue
		if left.Kind == ast.LiteralNumber {
			lf = float64(left.NumberValue)
		}
		if right.Kind == ast.LiteralNumber {
			rf = float64(right.NumberValue)
		}
		switch operator {
		case "+":
			return ast.Literal{Kind: ast.LiteralFloat, FloatValue: lf + rf}, nil
		case "-":
			return ast.Literal{Kind: ast.LiteralFloat, FloatValue: lf - rf}, nil
		case "*":
			return ast.Literal{Kind: ast.LiteralFloat, FloatValue: lf * rf}, nil
		case "/":
			if rf == 0 {
				return ast.Literal{}, fmt.Errorf("division by zero")
			}
			return ast.Literal{Kind: ast.LiteralFloat, FloatValue: lf / rf}, nil
		default:
			return ast.Literal{}, fmt.Errorf("unsupported arithmetic operator %s", operator)
		}
	}

	// Integer arithmetic
	if left.Kind != ast.LiteralNumber || right.Kind != ast.LiteralNumber {
		return ast.Literal{}, fmt.Errorf("arithmetic operands must be numeric")
	}

	switch operator {
	case "+":
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: left.NumberValue + right.NumberValue}, nil
	case "-":
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: left.NumberValue - right.NumberValue}, nil
	case "*":
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: left.NumberValue * right.NumberValue}, nil
	case "/":
		if right.NumberValue == 0 {
			return ast.Literal{}, fmt.Errorf("division by zero")
		}
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: left.NumberValue / right.NumberValue}, nil
	default:
		return ast.Literal{}, fmt.Errorf("unsupported arithmetic operator %s", operator)
	}
}

func literalKey(value ast.Literal) string {
	switch value.Kind {
	case ast.LiteralString:
		return "s:" + value.StringValue
	case ast.LiteralNumber:
		// Use AppendInt into stack buffer → single allocation (string conversion).
		var buf [22]byte // "n:" + max int64 = 20 digits + sign
		buf[0] = 'n'
		buf[1] = ':'
		return string(strconv.AppendInt(buf[:2], value.NumberValue, 10))
	case ast.LiteralNull:
		return "z:null"
	case ast.LiteralBoolean:
		if value.BoolValue {
			return "b:1"
		}
		return "b:0"
	case ast.LiteralFloat:
		var buf [32]byte
		buf[0] = 'f'
		buf[1] = ':'
		return string(strconv.AppendFloat(buf[:2], value.FloatValue, 'g', -1, 64))
	case ast.LiteralTimestamp:
		var buf [22]byte
		buf[0] = 't'
		buf[1] = ':'
		return string(strconv.AppendInt(buf[:2], value.NumberValue, 10))
	case ast.LiteralJSON:
		return "j:" + value.StringValue
	default:
		return "u:unknown"
	}
}

func extractJoinValue(row map[string]ast.Literal, tableName string, columnRef string) (ast.Literal, bool) {
	reference := strings.TrimSpace(strings.ToLower(columnRef))
	if reference == "" {
		return ast.Literal{}, false
	}

	parts := strings.Split(reference, ".")
	if len(parts) == 2 {
		if parts[0] != strings.ToLower(tableName) {
			return ast.Literal{}, false
		}
		value, exists := row[parts[1]]
		return value, exists
	}

	value, exists := row[reference]
	return value, exists
}

func literalEqual(left, right ast.Literal) bool {
	if left.Kind == ast.LiteralNull || right.Kind == ast.LiteralNull {
		return false
	}

	if left.Kind != right.Kind {
		return false
	}

	switch left.Kind {
	case ast.LiteralString:
		return left.StringValue == right.StringValue
	case ast.LiteralNumber:
		return left.NumberValue == right.NumberValue
	case ast.LiteralBoolean:
		return left.BoolValue == right.BoolValue
	case ast.LiteralFloat:
		return left.FloatValue == right.FloatValue
	case ast.LiteralTimestamp:
		return left.NumberValue == right.NumberValue
	case ast.LiteralJSON:
		return left.StringValue == right.StringValue
	case ast.LiteralNull:
		return true
	default:
		return false
	}
}

func mergeQualifiedRows(leftTable string, left map[string]ast.Literal, rightTable string, right map[string]ast.Literal) map[string]ast.Literal {
	merged := make(map[string]ast.Literal, len(left)+len(right)*2)

	for column, value := range left {
		qualified := leftTable + "." + column
		merged[qualified] = value
		if _, exists := merged[column]; !exists {
			merged[column] = value
		}
	}

	for column, value := range right {
		qualified := rightTable + "." + column
		merged[qualified] = value
		if _, exists := merged[column]; !exists {
			merged[column] = value
		}
	}

	return merged
}

func tableColumnNames(t *tableState) []string {
	return t.columns
}

func rowsEqual(a, b map[string]ast.Literal) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		bv, ok := b[k]
		if !ok {
			return false
		}
		if !literalEqual(v, bv) {
			return false
		}
	}
	return true
}

func compareLiterals(left, right ast.Literal) int {
	leftRank := literalRank(left.Kind)
	rightRank := literalRank(right.Kind)
	if leftRank != rightRank {
		if leftRank < rightRank {
			return -1
		}
		return 1
	}

	switch left.Kind {
	case ast.LiteralString:
		if left.StringValue < right.StringValue {
			return -1
		}
		if left.StringValue > right.StringValue {
			return 1
		}
		return 0
	case ast.LiteralNumber:
		if left.NumberValue < right.NumberValue {
			return -1
		}
		if left.NumberValue > right.NumberValue {
			return 1
		}
		return 0
	case ast.LiteralBoolean:
		lb, rb := 0, 0
		if left.BoolValue {
			lb = 1
		}
		if right.BoolValue {
			rb = 1
		}
		return lb - rb
	case ast.LiteralFloat:
		if left.FloatValue < right.FloatValue {
			return -1
		}
		if left.FloatValue > right.FloatValue {
			return 1
		}
		return 0
	case ast.LiteralTimestamp:
		if left.NumberValue < right.NumberValue {
			return -1
		}
		if left.NumberValue > right.NumberValue {
			return 1
		}
		return 0
	case ast.LiteralJSON:
		if left.StringValue < right.StringValue {
			return -1
		}
		if left.StringValue > right.StringValue {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func literalRank(kind ast.LiteralKind) int {
	switch kind {
	case ast.LiteralNull:
		return 0
	case ast.LiteralBoolean:
		return 1
	case ast.LiteralNumber:
		return 2
	case ast.LiteralFloat:
		return 3
	case ast.LiteralTimestamp:
		return 4
	case ast.LiteralString:
		return 5
	case ast.LiteralJSON:
		return 6
	default:
		return 7
	}
}

func matchPredicate(row map[string]ast.Literal, predicate *ast.Predicate, state *readableState, engine *Engine) bool {
	return evaluatePredicate3VL(row, predicate, state, engine) == ternaryTrue
}

// resolveJsonAccess evaluates a JSON path expression against a row.
// It extracts the column value from the row, parses it as JSON, walks the key path,
// and returns the result. TextMode (->>)  returns LiteralString; non-TextMode (->) returns LiteralJSON.
func resolveJsonAccess(row map[string]ast.Literal, access *ast.JsonAccess) (ast.Literal, bool) {
	if access == nil || len(access.Path) == 0 {
		return ast.Literal{}, false
	}

	col, exists := row[access.Column]
	if !exists {
		return ast.Literal{}, false
	}
	if col.Kind == ast.LiteralNull {
		return ast.Literal{Kind: ast.LiteralNull}, true
	}
	if col.Kind != ast.LiteralJSON {
		return ast.Literal{}, false
	}

	// Parse the JSON value.
	var current interface{}
	if err := json.Unmarshal([]byte(col.StringValue), &current); err != nil {
		return ast.Literal{}, false
	}

	// Walk the key path.
	for _, key := range access.Path {
		obj, ok := current.(map[string]interface{})
		if !ok {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		current, ok = obj[key]
		if !ok {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
	}

	// Convert the result.
	if current == nil {
		return ast.Literal{Kind: ast.LiteralNull}, true
	}

	if access.TextMode {
		// ->> returns text
		switch v := current.(type) {
		case string:
			return ast.Literal{Kind: ast.LiteralString, StringValue: v}, true
		case float64:
			if v == float64(int64(v)) {
				return ast.Literal{Kind: ast.LiteralString, StringValue: strconv.FormatInt(int64(v), 10)}, true
			}
			return ast.Literal{Kind: ast.LiteralString, StringValue: strconv.FormatFloat(v, 'f', -1, 64)}, true
		case bool:
			if v {
				return ast.Literal{Kind: ast.LiteralString, StringValue: "true"}, true
			}
			return ast.Literal{Kind: ast.LiteralString, StringValue: "false"}, true
		default:
			// Arrays/objects: marshal back to JSON string
			b, err := json.Marshal(current)
			if err != nil {
				return ast.Literal{}, false
			}
			return ast.Literal{Kind: ast.LiteralString, StringValue: string(b)}, true
		}
	}

	// -> returns JSON
	b, err := json.Marshal(current)
	if err != nil {
		return ast.Literal{}, false
	}
	return ast.Literal{Kind: ast.LiteralJSON, StringValue: string(b)}, true
}

// matchLikePattern implements SQL LIKE pattern matching.
// % matches any sequence of characters, _ matches any single character.
// The match is case-sensitive and deterministic.
func matchLikePattern(value, pattern string) bool {
	// Convert SQL LIKE pattern to Go regex.
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		switch ch {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		default:
			b.WriteString(regexp.QuoteMeta(string(ch)))
		}
	}
	b.WriteString("$")

	re, err := regexp.Compile(b.String())
	if err != nil {
		return false
	}
	return re.MatchString(value)
}

// evaluateRowFunction evaluates row-level functions: COALESCE, NULLIF.
// It returns the result and true if the expression matches a known function,
// or zero value and false otherwise.
func evaluateRowFunction(expr string, row map[string]ast.Literal) (ast.Literal, bool) {
	name, args, ok := parseRowFunctionCall(expr)
	if !ok {
		return ast.Literal{}, false
	}

	switch name {
	case "coalesce":
		for _, arg := range args {
			val, resolved := resolvePredicateOperand(row, arg)
			if !resolved {
				continue
			}
			if val.Kind != ast.LiteralNull {
				return val, true
			}
		}
		return ast.Literal{Kind: ast.LiteralNull}, true

	case "nullif":
		if len(args) != 2 {
			return ast.Literal{}, false
		}
		a, aOk := resolvePredicateOperand(row, args[0])
		b, bOk := resolvePredicateOperand(row, args[1])
		if !aOk {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		if !bOk {
			return a, true
		}
		if a.Kind != ast.LiteralNull && b.Kind != ast.LiteralNull && compareLiterals(a, b) == 0 {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		return a, true

	// ── String functions ────────────────────────────────────────────────

	case "upper":
		if len(args) != 1 {
			return ast.Literal{}, false
		}
		v := resolveStringArg(row, args[0])
		if v == nil {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		return ast.Literal{Kind: ast.LiteralString, StringValue: strings.ToUpper(*v)}, true

	case "lower":
		if len(args) != 1 {
			return ast.Literal{}, false
		}
		v := resolveStringArg(row, args[0])
		if v == nil {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		return ast.Literal{Kind: ast.LiteralString, StringValue: strings.ToLower(*v)}, true

	case "length":
		if len(args) != 1 {
			return ast.Literal{}, false
		}
		v := resolveStringArg(row, args[0])
		if v == nil {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(len(*v))}, true

	case "concat":
		if len(args) == 0 {
			return ast.Literal{Kind: ast.LiteralString, StringValue: ""}, true
		}
		var sb strings.Builder
		for _, arg := range args {
			v := resolveStringArg(row, arg)
			if v == nil {
				return ast.Literal{Kind: ast.LiteralNull}, true
			}
			sb.WriteString(*v)
		}
		return ast.Literal{Kind: ast.LiteralString, StringValue: sb.String()}, true

	case "trim":
		if len(args) != 1 {
			return ast.Literal{}, false
		}
		v := resolveStringArg(row, args[0])
		if v == nil {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		return ast.Literal{Kind: ast.LiteralString, StringValue: strings.TrimSpace(*v)}, true

	case "replace":
		if len(args) != 3 {
			return ast.Literal{}, false
		}
		v := resolveStringArg(row, args[0])
		old := resolveStringArg(row, args[1])
		nw := resolveStringArg(row, args[2])
		if v == nil || old == nil || nw == nil {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		return ast.Literal{Kind: ast.LiteralString, StringValue: strings.ReplaceAll(*v, *old, *nw)}, true

	case "left":
		if len(args) != 2 {
			return ast.Literal{}, false
		}
		v := resolveStringArg(row, args[0])
		n := resolveNumericArg(row, args[1])
		if v == nil || n == nil {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		count := int(*n)
		if count < 0 {
			count = 0
		}
		if count > len(*v) {
			count = len(*v)
		}
		return ast.Literal{Kind: ast.LiteralString, StringValue: (*v)[:count]}, true

	case "right":
		if len(args) != 2 {
			return ast.Literal{}, false
		}
		v := resolveStringArg(row, args[0])
		n := resolveNumericArg(row, args[1])
		if v == nil || n == nil {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		count := int(*n)
		if count < 0 {
			count = 0
		}
		if count > len(*v) {
			count = len(*v)
		}
		return ast.Literal{Kind: ast.LiteralString, StringValue: (*v)[len(*v)-count:]}, true

	case "substring":
		// substring(str, start) or substring(str, start, length)
		if len(args) < 2 || len(args) > 3 {
			return ast.Literal{}, false
		}
		v := resolveStringArg(row, args[0])
		startN := resolveNumericArg(row, args[1])
		if v == nil || startN == nil {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		start := int(*startN)
		if start < 1 {
			start = 1
		}
		startIdx := start - 1 // SQL is 1-based
		if startIdx > len(*v) {
			return ast.Literal{Kind: ast.LiteralString, StringValue: ""}, true
		}
		if len(args) == 3 {
			lenN := resolveNumericArg(row, args[2])
			if lenN == nil {
				return ast.Literal{Kind: ast.LiteralNull}, true
			}
			end := startIdx + int(*lenN)
			if end > len(*v) {
				end = len(*v)
			}
			return ast.Literal{Kind: ast.LiteralString, StringValue: (*v)[startIdx:end]}, true
		}
		return ast.Literal{Kind: ast.LiteralString, StringValue: (*v)[startIdx:]}, true

	// ── Math functions ──────────────────────────────────────────────────

	case "abs":
		if len(args) != 1 {
			return ast.Literal{}, false
		}
		return applyMathUnary(row, args[0], math.Abs), true

	case "ceil":
		if len(args) != 1 {
			return ast.Literal{}, false
		}
		return applyMathUnary(row, args[0], math.Ceil), true

	case "floor":
		if len(args) != 1 {
			return ast.Literal{}, false
		}
		return applyMathUnary(row, args[0], math.Floor), true

	case "round":
		if len(args) != 1 {
			return ast.Literal{}, false
		}
		return applyMathUnary(row, args[0], math.Round), true

	case "sqrt":
		if len(args) != 1 {
			return ast.Literal{}, false
		}
		return applyMathUnary(row, args[0], math.Sqrt), true

	case "power":
		if len(args) != 2 {
			return ast.Literal{}, false
		}
		base := resolveFloatArg(row, args[0])
		exp := resolveFloatArg(row, args[1])
		if base == nil || exp == nil {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		return ast.Literal{Kind: ast.LiteralFloat, FloatValue: math.Pow(*base, *exp)}, true

	case "mod":
		if len(args) != 2 {
			return ast.Literal{}, false
		}
		a := resolveNumericArg(row, args[0])
		b := resolveNumericArg(row, args[1])
		if a == nil || b == nil || *b == 0 {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: *a % *b}, true

	// ── Date/time functions ─────────────────────────────────────────────

	case "extract":
		// EXTRACT('part', ts_col) or EXTRACT(part FROM col) normalized to 2 args.
		if len(args) != 2 {
			return ast.Literal{}, false
		}
		part := strings.ToLower(strings.Trim(strings.TrimSpace(args[0]), "'\""))
		val, ok := resolvePredicateOperand(row, args[1])
		if !ok || val.Kind == ast.LiteralNull {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		if val.Kind != ast.LiteralTimestamp {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		ts := time.UnixMicro(val.NumberValue).UTC()
		var extracted int64
		switch part {
		case "year":
			extracted = int64(ts.Year())
		case "month":
			extracted = int64(ts.Month())
		case "day":
			extracted = int64(ts.Day())
		case "hour":
			extracted = int64(ts.Hour())
		case "minute":
			extracted = int64(ts.Minute())
		case "second":
			extracted = int64(ts.Second())
		case "dow", "dayofweek":
			extracted = int64(ts.Weekday())
		case "doy", "dayofyear":
			extracted = int64(ts.YearDay())
		case "epoch":
			return ast.Literal{Kind: ast.LiteralFloat, FloatValue: float64(val.NumberValue) / 1e6}, true
		default:
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: extracted}, true

	case "date_trunc":
		// DATE_TRUNC('part', ts_col)
		if len(args) != 2 {
			return ast.Literal{}, false
		}
		part := strings.ToLower(strings.Trim(strings.TrimSpace(args[0]), "'\""))
		val, ok := resolvePredicateOperand(row, args[1])
		if !ok || val.Kind == ast.LiteralNull {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		if val.Kind != ast.LiteralTimestamp {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		ts := time.UnixMicro(val.NumberValue).UTC()
		var truncated time.Time
		switch part {
		case "year":
			truncated = time.Date(ts.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
		case "month":
			truncated = time.Date(ts.Year(), ts.Month(), 1, 0, 0, 0, 0, time.UTC)
		case "day":
			truncated = time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC)
		case "hour":
			truncated = time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(), 0, 0, 0, time.UTC)
		case "minute":
			truncated = time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), 0, 0, time.UTC)
		case "second":
			truncated = time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), ts.Second(), 0, time.UTC)
		case "week":
			weekday := int(ts.Weekday())
			if weekday == 0 {
				weekday = 7
			}
			startOfWeek := ts.AddDate(0, 0, -(weekday - 1))
			truncated = time.Date(startOfWeek.Year(), startOfWeek.Month(), startOfWeek.Day(), 0, 0, 0, 0, time.UTC)
		default:
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		return ast.Literal{Kind: ast.LiteralTimestamp, NumberValue: truncated.UnixMicro()}, true

	default:
		return ast.Literal{}, false
	}
}

// parseRowFunctionCall detects function call patterns like "name(arg1, arg2, ...)"
// and splits them into the function name and arguments. Arguments are split at
// top-level commas (respecting parenthesis depth and string literals).
func parseRowFunctionCall(expr string) (string, []string, bool) {
	trimmed := strings.TrimSpace(expr)
	openParen := strings.Index(trimmed, "(")
	if openParen <= 0 {
		return "", nil, false
	}
	if trimmed[len(trimmed)-1] != ')' {
		return "", nil, false
	}

	name := strings.ToLower(strings.TrimSpace(trimmed[:openParen]))
	// Only accept known row-level functions.
	switch name {
	case "coalesce", "nullif",
		"upper", "lower", "length", "concat", "substring", "trim", "replace", "left", "right",
		"abs", "ceil", "floor", "round", "mod", "power", "sqrt",
		"extract", "date_trunc":
		// ok
	default:
		return "", nil, false
	}

	inner := trimmed[openParen+1 : len(trimmed)-1]

	// Special case: EXTRACT(part FROM col) → normalize to 2 args ["part", "col"].
	if name == "extract" {
		upperInner := strings.ToUpper(inner)
		if fromIdx := strings.Index(upperInner, " FROM "); fromIdx > 0 {
			part := strings.TrimSpace(inner[:fromIdx])
			col := strings.TrimSpace(inner[fromIdx+len(" FROM "):])
			return name, []string{part, col}, true
		}
	}

	args := splitFunctionArgs(inner)
	if len(args) == 0 {
		return "", nil, false
	}
	return name, args, true
}

// splitFunctionArgs splits a comma-separated argument list, respecting
// parenthesis depth and string literals.
func splitFunctionArgs(input string) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	inString := false

	for i := 0; i < len(input); i++ {
		ch := input[i]
		if ch == '\'' {
			inString = !inString
			current.WriteByte(ch)
			continue
		}
		if inString {
			current.WriteByte(ch)
			continue
		}
		switch ch {
		case '(':
			depth++
			current.WriteByte(ch)
		case ')':
			if depth > 0 {
				depth--
			}
			current.WriteByte(ch)
		case ',':
			if depth == 0 {
				part := strings.TrimSpace(current.String())
				if part != "" {
					parts = append(parts, part)
				}
				current.Reset()
			} else {
				current.WriteByte(ch)
			}
		default:
			current.WriteByte(ch)
		}
	}

	part := strings.TrimSpace(current.String())
	if part != "" {
		parts = append(parts, part)
	}
	return parts
}

// stripColumnAlias splits a column expression like "expr as alias" into
// (expr, alias). If no AS alias is present, it returns (column, column).
// The search for " as " only considers top-level occurrences — those outside
// parentheses and string literals.
func stripColumnAlias(column string) (string, string) {
	lower := strings.ToLower(column)
	depth := 0
	inString := false
	lastAs := -1
	for i := 0; i < len(lower); i++ {
		ch := lower[i]
		if ch == '\'' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' {
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth == 0 && i+4 <= len(lower) && lower[i:i+4] == " as " {
			lastAs = i
		}
	}
	if lastAs < 0 {
		return strings.TrimSpace(column), strings.TrimSpace(column)
	}
	expr := strings.TrimSpace(column[:lastAs])
	alias := strings.TrimSpace(column[lastAs+4:])
	if expr == "" || alias == "" {
		return strings.TrimSpace(column), strings.TrimSpace(column)
	}
	return expr, alias
}

// evaluateCaseWhenExpression evaluates a CASE WHEN expression against a row.
// Each branch condition is parsed as a WHERE clause predicate and evaluated
// in order. The THEN result of the first matching branch is returned.
// If no branch matches, the ELSE result (or NULL) is returned.
func evaluateCaseWhenExpression(row map[string]ast.Literal, caseExpr ast.CaseWhenExpression, state *readableState, engine *Engine) ast.Literal {
	for _, branch := range caseExpr.Branches {
		predicate, err := parser.ParsePredicate(branch.Condition)
		if err != nil {
			continue
		}

		if matchPredicate(row, predicate, state, engine) {
			return resolveCaseResult(row, branch.Result)
		}
	}

	if caseExpr.ElseResult != "" {
		return resolveCaseResult(row, caseExpr.ElseResult)
	}

	return ast.Literal{Kind: ast.LiteralNull}
}

// resolveCaseResult resolves a CASE WHEN result value. It handles string
// literals (quoted), numeric literals, boolean literals, and column references.
func resolveCaseResult(row map[string]ast.Literal, result string) ast.Literal {
	trimmed := strings.TrimSpace(result)
	if trimmed == "" {
		return ast.Literal{Kind: ast.LiteralNull}
	}

	// String literal: 'value'
	if len(trimmed) >= 2 && trimmed[0] == '\'' && trimmed[len(trimmed)-1] == '\'' {
		return ast.Literal{Kind: ast.LiteralString, StringValue: trimmed[1 : len(trimmed)-1]}
	}

	// NULL literal
	if strings.ToUpper(trimmed) == "NULL" {
		return ast.Literal{Kind: ast.LiteralNull}
	}

	// Boolean literals
	upper := strings.ToUpper(trimmed)
	if upper == "TRUE" {
		return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: true}
	}
	if upper == "FALSE" {
		return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: false}
	}

	// Numeric literal (integer)
	if num, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: num}
	}

	// Numeric literal (float)
	if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return ast.Literal{Kind: ast.LiteralFloat, FloatValue: f}
	}

	// Column reference
	canonical := strings.ToLower(trimmed)
	if val, ok := row[canonical]; ok {
		return val
	}

	// Row-level function
	if val, ok := evaluateRowFunction(canonical, row); ok {
		return val
	}

	// Fallback: return as string
	return ast.Literal{Kind: ast.LiteralString, StringValue: trimmed}
}

// evaluateInList evaluates col IN (val1, val2, ...) or NOT IN with literal values.
func evaluateInList(row map[string]ast.Literal, predicate *ast.Predicate, negate bool) ternaryResult {
	value, exists := resolvePredicateOperand(row, predicate.Column)
	if !exists {
		return ternaryFalse
	}
	if value.Kind == ast.LiteralNull {
		return ternaryUnknown
	}

	found := false
	hasNull := false
	for _, iv := range predicate.InValues {
		if iv.Kind == ast.LiteralNull {
			hasNull = true
			continue
		}
		if compareLiteralByOperator(value, "=", iv) {
			found = true
			break
		}
	}

	if negate {
		if found {
			return ternaryFalse
		}
		if hasNull {
			return ternaryUnknown
		}
		return ternaryTrue
	}
	if found {
		return ternaryTrue
	}
	if hasNull {
		return ternaryUnknown
	}
	return ternaryFalse
}

// evaluateBetween evaluates col BETWEEN lo AND hi or NOT BETWEEN.
func evaluateBetween(row map[string]ast.Literal, predicate *ast.Predicate, negate bool) ternaryResult {
	value, exists := resolvePredicateOperand(row, predicate.Column)
	if !exists {
		return ternaryFalse
	}
	if value.Kind == ast.LiteralNull {
		return ternaryUnknown
	}
	if predicate.Value2 == nil {
		return ternaryFalse
	}
	lo := predicate.Value
	hi := *predicate.Value2
	if lo.Kind == ast.LiteralNull || hi.Kind == ast.LiteralNull {
		return ternaryUnknown
	}

	geLo := compareLiteralByOperator(value, ">=", lo)
	leHi := compareLiteralByOperator(value, "<=", hi)
	inRange := geLo && leHi

	if negate {
		if inRange {
			return ternaryFalse
		}
		return ternaryTrue
	}
	if inRange {
		return ternaryTrue
	}
	return ternaryFalse
}

// evaluateCast handles CAST(expr AS type) expressions.
// Returns the cast result and true if the expression is a valid CAST, or
// zero value and false otherwise.
func evaluateCast(expr string, row map[string]ast.Literal) (ast.Literal, bool) {
	normalized := normalizeExpressionSyntax(expr)
	upper := strings.ToUpper(strings.TrimSpace(normalized))
	if !strings.HasPrefix(upper, "CAST(") || !strings.HasSuffix(upper, ")") {
		return ast.Literal{}, false
	}

	// Extract inner: expr AS type
	inner := strings.TrimSpace(normalized[5 : len(normalized)-1]) // strip "CAST(" and ")"
	asIdx := -1
	innerUpper := strings.ToUpper(inner)
	// Find the last " AS " to split (handles cases like column names containing "as")
	asIdx = strings.LastIndex(innerUpper, " AS ")
	if asIdx < 0 {
		return ast.Literal{}, false
	}

	sourceExpr := strings.TrimSpace(inner[:asIdx])
	targetType := strings.ToUpper(strings.TrimSpace(inner[asIdx+4:]))

	// Resolve source value: could be a column reference or a literal.
	val, resolved := resolvePredicateOperand(row, sourceExpr)
	if !resolved {
		return ast.Literal{Kind: ast.LiteralNull}, true
	}
	if val.Kind == ast.LiteralNull {
		return ast.Literal{Kind: ast.LiteralNull}, true
	}

	switch targetType {
	case "INT", "INTEGER", "BIGINT":
		switch val.Kind {
		case ast.LiteralNumber:
			return val, true
		case ast.LiteralFloat:
			return ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(val.FloatValue)}, true
		case ast.LiteralString:
			n, err := strconv.ParseInt(val.StringValue, 10, 64)
			if err != nil {
				f, err2 := strconv.ParseFloat(val.StringValue, 64)
				if err2 != nil {
					return ast.Literal{Kind: ast.LiteralNull}, true
				}
				return ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(f)}, true
			}
			return ast.Literal{Kind: ast.LiteralNumber, NumberValue: n}, true
		case ast.LiteralBoolean:
			if val.BoolValue {
				return ast.Literal{Kind: ast.LiteralNumber, NumberValue: 1}, true
			}
			return ast.Literal{Kind: ast.LiteralNumber, NumberValue: 0}, true
		}
	case "FLOAT", "REAL", "DOUBLE", "NUMERIC", "DECIMAL":
		switch val.Kind {
		case ast.LiteralNumber:
			return ast.Literal{Kind: ast.LiteralFloat, FloatValue: float64(val.NumberValue)}, true
		case ast.LiteralFloat:
			return val, true
		case ast.LiteralString:
			f, err := strconv.ParseFloat(val.StringValue, 64)
			if err != nil {
				return ast.Literal{Kind: ast.LiteralNull}, true
			}
			return ast.Literal{Kind: ast.LiteralFloat, FloatValue: f}, true
		}
	case "TEXT", "VARCHAR", "CHAR":
		switch val.Kind {
		case ast.LiteralString:
			return val, true
		case ast.LiteralNumber:
			return ast.Literal{Kind: ast.LiteralString, StringValue: strconv.FormatInt(val.NumberValue, 10)}, true
		case ast.LiteralFloat:
			return ast.Literal{Kind: ast.LiteralString, StringValue: strconv.FormatFloat(val.FloatValue, 'f', -1, 64)}, true
		case ast.LiteralBoolean:
			return ast.Literal{Kind: ast.LiteralString, StringValue: strconv.FormatBool(val.BoolValue)}, true
		}
	case "BOOLEAN", "BOOL":
		switch val.Kind {
		case ast.LiteralBoolean:
			return val, true
		case ast.LiteralNumber:
			return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: val.NumberValue != 0}, true
		case ast.LiteralString:
			upper := strings.ToUpper(val.StringValue)
			if upper == "TRUE" || upper == "1" {
				return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: true}, true
			}
			return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: false}, true
		}
	case "DATE":
		ts, ok := literalAsTime(val)
		if !ok {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		dateOnly := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC)
		return ast.Literal{Kind: ast.LiteralTimestamp, NumberValue: dateOnly.UnixMicro()}, true
	case "TIMESTAMP", "DATETIME":
		ts, ok := literalAsTime(val)
		if !ok {
			return ast.Literal{Kind: ast.LiteralNull}, true
		}
		return ast.Literal{Kind: ast.LiteralTimestamp, NumberValue: ts.UnixMicro()}, true
	}

	// Unsupported cast — return NULL.
	return ast.Literal{Kind: ast.LiteralNull}, true
}

func normalizeExpressionSyntax(expr string) string {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return trimmed
	}
	trimmed = collapsePostgresCastSpacing(trimmed)
	for {
		rewritten, changed := rewriteTopLevelPostgresCast(trimmed)
		if !changed {
			return trimmed
		}
		trimmed = rewritten
	}
}

func collapsePostgresCastSpacing(expr string) string {
	var b strings.Builder
	b.Grow(len(expr))
	inString := false
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '\'' {
			if inString && i+1 < len(expr) && expr[i+1] == '\'' {
				b.WriteByte(ch)
				b.WriteByte(ch)
				i++
				continue
			}
			inString = !inString
			b.WriteByte(ch)
			continue
		}
		if !inString && ch == ':' {
			j := i + 1
			for j < len(expr) && expr[j] == ' ' {
				j++
			}
			if j < len(expr) && expr[j] == ':' {
				b.WriteString("::")
				i = j
				for i+1 < len(expr) && expr[i+1] == ' ' {
					i++
				}
				continue
			}
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func rewriteTopLevelPostgresCast(expr string) (string, bool) {
	depth := 0
	inString := false
	castIdx := -1
	for i := 0; i+1 < len(expr); i++ {
		ch := expr[i]
		if ch == '\'' {
			if inString && i+1 < len(expr) && expr[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ':':
			if depth == 0 && expr[i+1] == ':' {
				castIdx = i
			}
		}
	}
	if castIdx < 0 {
		return expr, false
	}
	left := strings.TrimSpace(expr[:castIdx])
	right := strings.TrimSpace(expr[castIdx+2:])
	if left == "" || right == "" {
		return expr, false
	}
	return fmt.Sprintf("CAST(%s AS %s)", left, right), true
}

func literalAsTime(val ast.Literal) (time.Time, bool) {
	switch val.Kind {
	case ast.LiteralTimestamp:
		return time.UnixMicro(val.NumberValue).UTC(), true
	case ast.LiteralString:
		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
		}
		for _, layout := range layouts {
			parsed, err := time.Parse(layout, val.StringValue)
			if err == nil {
				return parsed.UTC(), true
			}
		}
	}
	return time.Time{}, false
}

// ── Helper functions for string / math row-level functions ──────────────

// resolveStringArg resolves a function argument to its string representation.
// Returns nil if the value is NULL or unresolvable.
func resolveStringArg(row map[string]ast.Literal, arg string) *string {
	val, ok := resolvePredicateOperand(row, arg)
	if !ok || val.Kind == ast.LiteralNull {
		return nil
	}
	var s string
	switch val.Kind {
	case ast.LiteralString:
		s = val.StringValue
	case ast.LiteralNumber:
		s = strconv.FormatInt(val.NumberValue, 10)
	case ast.LiteralFloat:
		s = strconv.FormatFloat(val.FloatValue, 'f', -1, 64)
	case ast.LiteralBoolean:
		s = strconv.FormatBool(val.BoolValue)
	default:
		return nil
	}
	return &s
}

// resolveNumericArg resolves a function argument to an int64.
// Returns nil if the value is NULL, unresolvable, or not numeric.
func resolveNumericArg(row map[string]ast.Literal, arg string) *int64 {
	val, ok := resolvePredicateOperand(row, arg)
	if !ok || val.Kind == ast.LiteralNull {
		return nil
	}
	var n int64
	switch val.Kind {
	case ast.LiteralNumber:
		n = val.NumberValue
	case ast.LiteralFloat:
		n = int64(val.FloatValue)
	case ast.LiteralString:
		parsed, err := strconv.ParseInt(val.StringValue, 10, 64)
		if err != nil {
			return nil
		}
		n = parsed
	default:
		return nil
	}
	return &n
}

// resolveFloatArg resolves a function argument to a float64.
// Returns nil if the value is NULL, unresolvable, or not numeric.
func resolveFloatArg(row map[string]ast.Literal, arg string) *float64 {
	val, ok := resolvePredicateOperand(row, arg)
	if !ok || val.Kind == ast.LiteralNull {
		return nil
	}
	var f float64
	switch val.Kind {
	case ast.LiteralFloat:
		f = val.FloatValue
	case ast.LiteralNumber:
		f = float64(val.NumberValue)
	case ast.LiteralString:
		parsed, err := strconv.ParseFloat(val.StringValue, 64)
		if err != nil {
			return nil
		}
		f = parsed
	default:
		return nil
	}
	return &f
}

// applyMathUnary resolves a single numeric argument and applies fn.
// Returns integer result if the input was integer and the result has no fraction,
// otherwise float.
func applyMathUnary(row map[string]ast.Literal, arg string, fn func(float64) float64) ast.Literal {
	val, ok := resolvePredicateOperand(row, arg)
	if !ok || val.Kind == ast.LiteralNull {
		return ast.Literal{Kind: ast.LiteralNull}
	}
	var f float64
	switch val.Kind {
	case ast.LiteralNumber:
		f = float64(val.NumberValue)
	case ast.LiteralFloat:
		f = val.FloatValue
	default:
		return ast.Literal{Kind: ast.LiteralNull}
	}
	result := fn(f)
	// If input was integer and the result is a whole number, keep as integer.
	if val.Kind == ast.LiteralNumber && result == math.Trunc(result) {
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(result)}
	}
	return ast.Literal{Kind: ast.LiteralFloat, FloatValue: result}
}
