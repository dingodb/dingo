lexer grammar DingoExprLexer;

INT             : NUM ;
REAL            : (NUM '.' NUM EXP? | NUM EXP) ;
STR             : '\'' (ESC | ~['\\])* '\'' | '"' (ESC | ~["\\])* '"' ;
BOOL            : 'true' | 'false' ;

// operators
ADD             : '+' ;
SUB             : '-' ;
MUL             : '*' ;
DIV             : '/' ;

LT              : '<' ;
LE              : '<=' ;
EQ              : '==' | '=' ;
GT              : '>' ;
GE              : '>=' ;
NE              : '<>' | '!=' ;

AND             : 'and' | '&&' ;
OR              : 'or' | '||' ;
NOT             : 'not' | '!' ;

STARTS_WITH     : 'startsWith' ;
ENDS_WITH       : 'endsWith' ;
CONTAINS        : 'contains' ;
MATCHES         : 'matches' ;

ID              : (ALPHA | '_' | '$') (ALPHA | DIGIT | '_' )* ;

WS              : [ \t]+ -> skip ;
NL              : ('\r'? '\n')+ -> skip ;

LPAR            : '(' ;
RPAR            : ')' ;
COMMA           : ',' ;
DOT             : '.' ;
LBRCK           : '[' ;
RBRCK           : ']' ;

fragment
ALPHA           : [a-zA-Z] ;

fragment
DIGIT           : [0-9] ;

fragment
HEX             : [0-9a-fA-F] ;

fragment
NUM             : DIGIT+ ;

fragment
EXP             : [Ee] [+\-]? NUM ;

fragment
ESC             : '\\' (["\\/bfnrt] | UNICODE) ;

fragment
UNICODE         : 'u' HEX HEX HEX HEX ;
