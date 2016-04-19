grammar Conditions;
WS: [ \t\r\n]+ -> skip ;
ID: [a-zA-Z][a-zA-Z_0-9]*;
Number: [0-9.]+;
value: value ('*'|'/'|'%') value
	 | value ('+'|'-') value
	 | '(' value ')'
	 | ID;
expression: | value ':' value
		  |	value '>'  value
	 	  | value '<'  value
	 	  | value '='  value
	 	  | value '>=' value
	 	  | value '<=' value
	 	  | value '<>' value
		  |	value '>'  Number
	 	  | value '<'  Number
	 	  | value '='  Number
	 	  | value '>=' Number
	 	  | value '<=' Number
	 	  | value '<>' Number; 
predicates: expression 'AND' expression 
		  | expression 'OR' expression
		  | expression 'XOR' expression
		  | 'NOT' expression
		  | '(' expression ')';
	 	  
