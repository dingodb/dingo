parser grammar DingoExprParser;

options { tokenVocab=DingoExprLexer; }

expr : (ADD | SUB)? INT                                              # Int
     | (ADD | SUB)? REAL                                             # Real
     | STR                                                           # Str
     | BOOL                                                          # Bool
     | ID                                                            # Var
     | LPAR expr RPAR                                                # Pars
     | fun=ID LPAR (expr (COMMA expr) *) ? RPAR                      # Fun
     | expr DOT ID                                                   # StrIndex
     | expr LBRCK expr RBRCK                                         # Index
     | op=(ADD | SUB) expr                                           # PosNeg
     | expr op=(MUL | DIV) expr                                      # MulDiv
     | expr op=(ADD | SUB) expr                                      # AddSub
     | expr op=(LT | LE | EQ | GT | GE | NE) expr                    # Relation
     | expr op=(STARTS_WITH | ENDS_WITH | CONTAINS | MATCHES) expr   # StringOp
     | op=NOT expr                                                   # Not
     | expr op=AND expr                                              # And
     | expr op=OR expr                                               # Or
     ;
