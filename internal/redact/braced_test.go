package redact

import "testing"

// Corpus for #534: MS SQL Server ODBC/ADO.NET/JDBC braced values,
// password={...}, which may hold ';', '@', blanks and '}}' (an escaped '}').
// The credentials are dummies.
var bracedLeaks = []secretRow{
	{"ticket", "password={Br@ce;Pw42x}", []string{"Pw42x", "Br@ce"}},
	{"ticket short", "Pwd={Ab;Cd9}", []string{"Cd9"}},
	{"PWD", "PWD={Br@ce;Pw42x}", []string{"Pw42x"}},
	{"pwd", "pwd={Br@ce;Pw42x}", []string{"Pw42x"}},
	{"Password", "Password={Br@ce;Pw42x}", []string{"Pw42x"}},
	{"secret", "secret={Br@ce;Pw42x}", []string{"Pw42x"}},
	{"ado.net string", "Server=db;Database=orders;User Id=sa;Password={Br@ce;Pw42x};Encrypt=true", []string{"Pw42x"}},
	{"odbc string", "Driver={ODBC Driver 18 for SQL Server};Server=db;UID=sa;PWD={Br@ce;Pw42x};", []string{"Pw42x"}},
	{"jdbc url", "jdbc:sqlserver://db:1433;databaseName=orders;user=sa;password={Br@ce;Pw42x};encrypt=true", []string{"Pw42x"}},
	{"jdbc url in json", `"connection.url":"jdbc:sqlserver://db;password={Br@ce;Pw42x}"`, []string{"Pw42x"}},
	{"escaped brace", "PWD={Sp ace}}Pw42x;x};Driver={ODBC Driver 18}", []string{"Pw42x", "ace"}},
	{"blanks", "Password = {Sp ace;Pw42x}", []string{"Pw42x", "ace"}},
	{"unclosed at end of line", "password={Br@ce;Pw42x", []string{"Pw42x"}},
	{"unclosed then next line", "password={Br@ce;Pw42x\nnext line orders", []string{"Pw42x"}},
	{"at only", "Password={Pw@42xQz}", []string{"42xQz"}},
	{"leading semicolon", "Password={;Pw42xQz}", []string{"Pw42xQz"}},
	{"in error text", "Login failed for user 'sa'. ConnectionString: Server=db;Pwd={Br@ce;Pw42x}; (SQLSTATE 28000)", []string{"Pw42x"}},
	{"oracle sibling", "jdbc:oracle:thin:loader/Pw9Orc@wh:1521:ORCL;password={Br@ceOrcT4il}", []string{"Pw9Orc", "OrcT4il"}},
	{"two values", "Pwd={Ab;Cd9x};Server=db;Password={Ef@Gh7y}", []string{"Cd9x", "Gh7y"}},
	{"camel key", "dbPassword={Br@ce;Pw42x}", []string{"Pw42x"}},
}

var bracedKeep = []string{
	"Driver={ODBC Driver 18 for SQL Server};Server=db;Database=orders",
	"Server={db};Database={orders}",
	"password={}",
	"a password={[redacted 8]} b",
}

var bracedExact = []struct{ in, want string }{
	{"password={Br@ce;Pw42x}", "password=[redacted 16]"},
	{"Pwd={Ab;Cd9}", "Pwd=[redacted 8]"},
	{"Server=db;Database=orders;User Id=sa;Password={Br@ce;Pw42x};Encrypt=true", "Server=db;Database=orders;User Id=sa;Password=[redacted 16];Encrypt=true"},
	{"Driver={ODBC Driver 18 for SQL Server};Server=db;UID=sa;PWD={Br@ce;Pw42x};", "Driver={ODBC Driver 18 for SQL Server};Server=db;UID=sa;PWD=[redacted 16];"},
	{"PWD={Sp ace}}Pw42x;x};Driver={ODBC Driver 18}", "PWD=[redacted 24];Driver={ODBC Driver 18}"},
	{"jdbc:sqlserver://db:1433;databaseName=orders;user=sa;password={Br@ce;Pw42x};encrypt=true", "jdbc:sqlserver://db:[redacted 64];encrypt=true"},
}

func TestBraced534(t *testing.T) {
	checkSecretRows(t, bracedLeaks)
	checkKeep(t, bracedKeep)
	checkExact(t, bracedExact)
}
